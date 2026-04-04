//! Integration tests for the example applications.
//!
//! These tests verify that each example's task handler works correctly
//! with a real PostgreSQL backend. They are marked `#[ignore]` and run
//! via `just test-examples`.

#[cfg(test)]
mod example_tests {
    use scheduler::PostgresScheduler;
    use scheduler::{ExecutionStatus, Scheduler as _};
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;
    use zart::{DurableScheduler, TaskRegistry, Worker, WorkerConfig};

    fn pg_url() -> String {
        std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://zart:zart@localhost:5432/zart".to_string())
    }

    async fn setup() -> Arc<PostgresScheduler> {
        let pool = sqlx::PgPool::connect(&pg_url())
            .await
            .expect("failed to connect to PostgreSQL");
        let scheduler = Arc::new(PostgresScheduler::new(pool));
        scheduler.run_migrations().await.expect("migrations failed");
        scheduler
    }

    fn spawn_worker(
        scheduler: Arc<PostgresScheduler>,
        registry: Arc<TaskRegistry<PostgresScheduler>>,
    ) -> Arc<Worker<PostgresScheduler>> {
        let config = WorkerConfig {
            poll_interval: Duration::from_millis(200),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 4,
            shutdown_timeout: Duration::from_secs(5),
            orphan_timeout: Duration::from_secs(30),
        };
        let worker = Arc::new(Worker::new(scheduler, registry, config));
        let w = worker.clone();
        tokio::spawn(async move { w.run().await });
        worker
    }

    // ── Example 1: Brewery Finder ─────────────────────────────────────────────

    mod brewery_finder {
        use super::*;

        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct FinderInput {
            zip_code: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct BreweryInfo {
            name: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct FinderOutput {
            zip_code: String,
            city: String,
            state: String,
            breweries: Vec<BreweryInfo>,
            found_at: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct ZipInfo {
            #[serde(rename = "place name")]
            place_name: String,
            state: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct BreweryRaw {
            name: String,
        }

        struct BreweryFinderTask;

        #[async_trait::async_trait]
        impl zart::registry::TaskHandler for BreweryFinderTask {
            type Data = FinderInput;
            type Output = FinderOutput;

            async fn run<S: scheduler::Scheduler>(
                &self,
                ctx: &mut zart::context::TaskContext<S>,
                data: Self::Data,
            ) -> Result<Self::Output, zart::error::TaskError> {
                let client = reqwest::Client::new();

                // Step 1: Look up ZIP code
                let zip_info: ZipInfo = ctx
                    .step_with_retry(
                        "lookup-zip",
                        zart::RetryConfig::exponential(3, Duration::from_millis(100)),
                        || {
                            let client = client.clone();
                            let zip = data.zip_code.clone();
                            async move {
                                let resp = client
                                    .get(format!("https://api.zippopotam.us/us/{zip}"))
                                    .send()
                                    .await
                                    .map_err(|e| zart::error::StepError::Failed {
                                        step: "lookup-zip".to_string(),
                                        reason: e.to_string(),
                                    })?;
                                let info: ZipInfo = resp.json().await.map_err(|e| {
                                    zart::error::StepError::Failed {
                                        step: "lookup-zip".to_string(),
                                        reason: format!("parse error: {e}"),
                                    }
                                })?;
                                Ok(info)
                            }
                        },
                    )
                    .await?;

                let city = zip_info.place_name.clone();
                let state = zip_info.state.clone();

                // Step 2: Find breweries
                let raw: Vec<BreweryRaw> = ctx
                    .step_with_retry(
                        "find-breweries",
                        zart::RetryConfig::exponential(3, Duration::from_millis(100)),
                        || {
                            let client = client.clone();
                            let city = city.clone();
                            async move {
                                let resp = client
                                    .get("https://api.openbrewerydb.org/v1/breweries")
                                    .query(&[("by_city", &city)])
                                    .send()
                                    .await
                                    .map_err(|e| zart::error::StepError::Failed {
                                        step: "find-breweries".to_string(),
                                        reason: e.to_string(),
                                    })?;
                                let breweries: Vec<BreweryRaw> =
                                    resp.json().await.map_err(|e| {
                                        zart::error::StepError::Failed {
                                            step: "find-breweries".to_string(),
                                            reason: format!("parse error: {e}"),
                                        }
                                    })?;
                                Ok(breweries)
                            }
                        },
                    )
                    .await?;

                // Step 3: Transform to output type
                let breweries: Vec<BreweryInfo> = ctx
                    .step("transform", || {
                        let raw = raw.clone();
                        async move {
                            Ok(raw
                                .into_iter()
                                .map(|b| BreweryInfo { name: b.name })
                                .collect())
                        }
                    })
                    .await?;

                Ok(FinderOutput {
                    zip_code: data.zip_code,
                    city,
                    state,
                    breweries,
                    found_at: chrono::Utc::now().to_rfc3339(),
                })
            }
        }

        #[tokio::test]
        #[ignore = "requires PostgreSQL and internet — run with: just test-examples"]
        async fn brewery_finder_completes_successfully() {
            let scheduler = setup().await;

            let mut registry = TaskRegistry::new();
            registry.register("brewery-finder-test", BreweryFinderTask);
            let registry = Arc::new(registry);

            let execution_id = format!("test-brewery-{}", Uuid::new_v4());
            let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

            let input = FinderInput {
                zip_code: "90210".to_string(),
            };
            durable
                .start_typed(&execution_id, "brewery-finder-test", &input)
                .await
                .expect("start failed");

            let worker = spawn_worker(scheduler.clone(), registry);

            let record = durable
                .wait(&execution_id, Duration::from_secs(60), None)
                .await
                .expect("wait failed");

            worker.stop();

            assert_eq!(record.status, ExecutionStatus::Completed);
            let result: FinderOutput =
                serde_json::from_value(record.result.expect("expected result"))
                    .expect("deserialize failed");
            assert_eq!(result.zip_code, "90210");
            assert!(!result.breweries.is_empty());
        }
    }

    // ── Example 2: Approval Workflow ──────────────────────────────────────────

    mod approval_workflow {
        use super::*;

        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct ApprovalRequest {
            zip_code: String,
            requester_name: String,
        }

        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct ApprovalDecision {
            approved: bool,
            reviewer: String,
            comment: String,
        }

        struct ApprovalTask;

        #[async_trait::async_trait]
        impl zart::registry::TaskHandler for ApprovalTask {
            type Data = ApprovalRequest;
            type Output = serde_json::Value;

            async fn run<S: scheduler::Scheduler>(
                &self,
                ctx: &mut zart::context::TaskContext<S>,
                data: Self::Data,
            ) -> Result<Self::Output, zart::error::TaskError> {
                // Step 1: Fetch location
                let city: String = ctx
                    .step("fetch-location", || {
                        let zip = data.zip_code.clone();
                        async move {
                            let client = reqwest::Client::new();
                            let resp = client
                                .get(format!("https://api.zippopotam.us/us/{zip}"))
                                .send()
                                .await
                                .map_err(|e| zart::error::StepError::Failed {
                                    step: "fetch-location".to_string(),
                                    reason: e.to_string(),
                                })?;
                            let json: serde_json::Value = resp.json().await.map_err(|e| {
                                zart::error::StepError::Failed {
                                    step: "fetch-location".to_string(),
                                    reason: format!("parse error: {e}"),
                                }
                            })?;
                            let place = json["place name"]
                                .as_str()
                                .unwrap_or("Unknown")
                                .to_string();
                            Ok(place)
                        }
                    })
                    .await?;

                // Step 2: Wait for approval
                let decision: ApprovalDecision = ctx
                    .wait_for_event("manager-approval", Some(Duration::from_secs(30)))
                    .await?;

                Ok(serde_json::json!({
                    "decision": if decision.approved { "approved" } else { "rejected" },
                    "city": city,
                    "reviewer": decision.reviewer,
                    "comment": decision.comment,
                }))
            }
        }

        #[tokio::test]
        #[ignore = "requires PostgreSQL and internet — run with: just test-examples"]
        async fn approval_example_completes_after_event() {
            let scheduler = setup().await;

            let mut registry = TaskRegistry::new();
            registry.register("approval-example", ApprovalTask);
            let registry = Arc::new(registry);

            let execution_id = format!("test-approval-{}", Uuid::new_v4());
            let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

            let request = ApprovalRequest {
                zip_code: "10001".to_string(),
                requester_name: "TestRequester".to_string(),
            };
            durable
                .start_typed(&execution_id, "approval-example", &request)
                .await
                .expect("start failed");

            let worker = spawn_worker(scheduler.clone(), registry);

            // Wait for the execution to park itself
            tokio::time::sleep(Duration::from_millis(1000)).await;

            // Deliver approval event
            let decision = ApprovalDecision {
                approved: true,
                reviewer: "TestManager".to_string(),
                comment: "Approved for testing".to_string(),
            };
            durable
                .offer_event(
                    &execution_id,
                    "manager-approval",
                    serde_json::to_value(&decision).unwrap(),
                )
                .await
                .expect("offer_event failed");

            let record = durable
                .wait(&execution_id, Duration::from_secs(30), None)
                .await
                .expect("wait failed");

            worker.stop();

            assert_eq!(record.status, ExecutionStatus::Completed);
            let result = record.result.expect("expected result");
            assert_eq!(result["decision"], "approved");
            assert_eq!(result["city"], "New York");
        }
    }

    // ── Example 3: Parallel Steps ─────────────────────────────────────────────

    mod parallel_steps {
        use super::*;

        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct ParallelInput {
            zip_codes: Vec<String>,
        }

        struct ParallelTask;

        #[async_trait::async_trait]
        impl zart::registry::TaskHandler for ParallelTask {
            type Data = ParallelInput;
            type Output = serde_json::Value;

            async fn run<S: scheduler::Scheduler>(
                &self,
                ctx: &mut zart::context::TaskContext<S>,
                data: Self::Data,
            ) -> Result<Self::Output, zart::error::TaskError> {
                // Pre-build owned copies so closures can be 'static.
                let zip_pairs: Vec<(usize, String)> = data
                    .zip_codes
                    .iter()
                    .enumerate()
                    .map(|(i, z)| (i, z.clone()))
                    .collect();

                // Schedule parallel ZIP lookups
                let mut handles = vec![];
                for (i, zip) in zip_pairs {
                    let handle = ctx.schedule_step(&format!("zip-{i}"), {
                        move || {
                            let zip = zip.clone();
                            async move {
                                let client = reqwest::Client::new();
                                let resp = client
                                    .get(format!("https://api.zippopotam.us/us/{zip}"))
                                    .send()
                                    .await
                                    .map_err(|e| zart::error::StepError::Failed {
                                        step: format!("zip-{zip}"),
                                        reason: e.to_string(),
                                    })?;
                                let json: serde_json::Value = resp.json().await.map_err(|e| {
                                    zart::error::StepError::Failed {
                                        step: format!("zip-{zip}"),
                                        reason: format!("parse error: {e}"),
                                    }
                                })?;
                                let city = json["place name"]
                                    .as_str()
                                    .unwrap_or("Unknown")
                                    .to_string();
                                Ok((zip, city))
                            }
                        }
                    });
                    handles.push(handle);
                }

                let results = ctx.wait_all(handles).await?;
                let mut cities: Vec<(String, String)> = vec![];
                for result in results {
                    let (zip, city) = result.map_err(|e| zart::error::TaskError::StepFailed {
                        step: "parallel-lookup".to_string(),
                        source: e,
                    })?;
                    cities.push((zip, city));
                }

                // Pre-build city indices for the second parallel phase.
                let city_indices: Vec<usize> = (0..cities.len()).collect();

                // Schedule parallel brewery searches
                let mut brewery_handles = vec![];
                let cities_for_closure = cities.clone();
                for i in city_indices {
                    let handle = ctx.schedule_step(&format!("breweries-{i}"), {
                        let cities = cities_for_closure.clone();
                        move || {
                            let city = cities[i].1.clone();
                            async move {
                                let client = reqwest::Client::new();
                                let resp = client
                                    .get("https://api.openbrewerydb.org/v1/breweries")
                                    .query(&[("by_city", &city)])
                                    .send()
                                    .await
                                    .map_err(|e| zart::error::StepError::Failed {
                                        step: format!("breweries-{city}"),
                                        reason: e.to_string(),
                                    })?;
                                let breweries: Vec<serde_json::Value> =
                                    resp.json().await.map_err(|e| {
                                        zart::error::StepError::Failed {
                                            step: format!("breweries-{city}"),
                                            reason: format!("parse error: {e}"),
                                        }
                                    })?;
                                Ok(breweries.len())
                            }
                        }
                    });
                    brewery_handles.push(handle);
                }

                let brewery_results = ctx.wait_all(brewery_handles).await?;
                let mut total = 0;
                for result in brewery_results {
                    let count = result.map_err(|e| zart::error::TaskError::StepFailed {
                        step: "parallel-breweries".to_string(),
                        source: e,
                    })?;
                    total += count;
                }

                Ok(serde_json::json!({
                    "cities_processed": cities.len(),
                    "total_breweries": total,
                }))
            }
        }

        #[tokio::test]
        #[ignore = "requires PostgreSQL and internet — run with: just test-examples"]
        async fn parallel_example_completes_all_steps() {
            let scheduler = setup().await;

            let mut registry = TaskRegistry::new();
            registry.register("parallel-example", ParallelTask);
            let registry = Arc::new(registry);

            let execution_id = format!("test-parallel-{}", Uuid::new_v4());
            let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

            let input = ParallelInput {
                zip_codes: vec![
                    "90210".to_string(),
                    "10001".to_string(),
                    "60601".to_string(),
                ],
            };
            durable
                .start_typed(&execution_id, "parallel-example", &input)
                .await
                .expect("start failed");

            let worker = spawn_worker(scheduler.clone(), registry);

            let record = durable
                .wait(&execution_id, Duration::from_secs(60), None)
                .await
                .expect("wait failed");

            worker.stop();

            assert_eq!(record.status, ExecutionStatus::Completed);
            let result = record.result.expect("expected result");
            assert_eq!(result["cities_processed"], 3);
            assert!(result["total_breweries"].as_u64().unwrap_or(0) > 0);
        }
    }
}

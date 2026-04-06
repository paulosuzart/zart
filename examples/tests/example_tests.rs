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

    fn spawn_worker(scheduler: Arc<PostgresScheduler>, registry: Arc<TaskRegistry>) -> Arc<Worker> {
        let config = WorkerConfig {
            poll_interval: Duration::from_millis(200),
            max_tasks_per_poll: 10,
            max_concurrent_tasks: 4,
            shutdown_timeout: Duration::from_secs(5),
            orphan_timeout: Duration::from_secs(30),
            ..Default::default()
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
            brewery_type: String,
            city: String,
            state: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct FinderOutput {
            zip_code: String,
            city: String,
            state: String,
            breweries: Vec<BreweryInfo>,
            found_at: String,
        }

        #[derive(Debug, Clone, serde::Deserialize)]
        struct PlaceInfo {
            #[serde(rename = "place name")]
            place_name: String,
            state: String,
        }

        #[derive(Debug, Clone, serde::Deserialize)]
        struct ZipResponse {
            places: Vec<PlaceInfo>,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct BreweryRaw {
            name: String,
            #[serde(default)]
            brewery_type: Option<String>,
            city: Option<String>,
            #[serde(default)]
            state: Option<String>,
        }

        struct BreweryFinderTask;

        #[async_trait::async_trait]
        impl zart::registry::TaskHandler for BreweryFinderTask {
            type Data = FinderInput;
            type Output = FinderOutput;

            async fn run(
                &self,
                ctx: &mut zart::context::TaskContext,
                data: Self::Data,
            ) -> Result<Self::Output, zart::error::TaskError> {
                let client = reqwest::Client::new();

                // Step 1: Look up ZIP code
                let (city, state) = ctx
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
                                let zip_resp: ZipResponse = resp.json().await.map_err(|e| {
                                    zart::error::StepError::Failed {
                                        step: "lookup-zip".to_string(),
                                        reason: format!("parse error: {e}"),
                                    }
                                })?;
                                let place = zip_resp.places.first().ok_or_else(|| {
                                    zart::error::StepError::Failed {
                                        step: "lookup-zip".to_string(),
                                        reason: format!("no place found for ZIP {zip}"),
                                    }
                                })?;
                                Ok((place.place_name.clone(), place.state.clone()))
                            }
                        },
                    )
                    .await?;

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
                        let city = city.clone();
                        let state = state.clone();
                        async move {
                            Ok(raw
                                .into_iter()
                                .map(|b| BreweryInfo {
                                    name: b.name,
                                    brewery_type: b
                                        .brewery_type
                                        .unwrap_or_else(|| "unknown".to_string()),
                                    city: b.city.unwrap_or_else(|| city.clone()),
                                    state: b.state.unwrap_or_else(|| state.clone()),
                                })
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
            requester_name: String,
            resource: String,
            reason: String,
        }

        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct ApprovalDecision {
            approved: bool,
            reviewer: String,
            comment: String,
        }

        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct ApprovalOutput {
            decision: String,
            requester: String,
            resource: String,
            reviewer: String,
            comment: String,
        }

        struct ApprovalTask;

        #[async_trait::async_trait]
        impl zart::registry::TaskHandler for ApprovalTask {
            type Data = ApprovalRequest;
            type Output = ApprovalOutput;

            async fn run(
                &self,
                ctx: &mut zart::context::TaskContext,
                data: Self::Data,
            ) -> Result<Self::Output, zart::error::TaskError> {
                // Step 1: Validate request (fake step)
                ctx.step("validate-request", || {
                    let name = data.requester_name.clone();
                    async move {
                        if name.is_empty() {
                            return Err(zart::error::StepError::Failed {
                                step: "validate-request".to_string(),
                                reason: "empty name".to_string(),
                            });
                        }
                        Ok(format!("Validated request from {name}"))
                    }
                })
                .await?;

                // Step 2: Wait for approval
                let decision: ApprovalDecision = ctx
                    .wait_for_event("manager-approval", Some(Duration::from_secs(30)))
                    .await?;

                Ok(ApprovalOutput {
                    decision: if decision.approved {
                        "approved".to_string()
                    } else {
                        "rejected".to_string()
                    },
                    requester: data.requester_name,
                    resource: data.resource,
                    reviewer: decision.reviewer,
                    comment: decision.comment,
                })
            }
        }

        #[tokio::test]
        #[ignore = "requires PostgreSQL — run with: just test-examples"]
        async fn approval_example_completes_after_event() {
            let scheduler = setup().await;

            let mut registry = TaskRegistry::new();
            registry.register("approval-example", ApprovalTask);
            let registry = Arc::new(registry);

            let execution_id = format!("test-approval-{}", Uuid::new_v4());
            let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

            let request = ApprovalRequest {
                requester_name: "TestRequester".to_string(),
                resource: "test-resource".to_string(),
                reason: "testing".to_string(),
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
            let result: ApprovalOutput =
                serde_json::from_value(record.result.expect("expected result"))
                    .expect("deserialize failed");
            assert_eq!(result.decision, "approved");
            assert_eq!(result.requester, "TestRequester");
        }
    }

    // ── Example 3: Parallel Steps ─────────────────────────────────────────────

    mod parallel_steps {
        use super::*;

        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct ParallelInput {
            services: Vec<String>,
        }

        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct ServiceResult {
            name: String,
            status: String,
            response_ms: u64,
            issues: Vec<String>,
        }

        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct ParallelOutput {
            services_checked: usize,
            total_issues: usize,
            results: Vec<ServiceResult>,
        }

        struct ParallelTask;

        #[async_trait::async_trait]
        impl zart::registry::TaskHandler for ParallelTask {
            type Data = ParallelInput;
            type Output = ParallelOutput;

            async fn run(
                &self,
                ctx: &mut zart::context::TaskContext,
                data: Self::Data,
            ) -> Result<Self::Output, zart::error::TaskError> {
                let mut handles = vec![];
                for service in &data.services {
                    let handle = ctx.schedule_step(&format!("check-{service}"), {
                        let service = service.clone();
                        move || {
                            let service = service.clone();
                            async move {
                                let (status, response_ms, issues) = match service.as_str() {
                                    "auth-api" => ("healthy".to_string(), 42, vec![]),
                                    "payments" => (
                                        "degraded".to_string(),
                                        156,
                                        vec!["high latency".to_string()],
                                    ),
                                    "users-db" => ("healthy".to_string(), 28, vec![]),
                                    _ => (
                                        "unknown".to_string(),
                                        0,
                                        vec!["no check configured".to_string()],
                                    ),
                                };
                                Ok(ServiceResult {
                                    name: service,
                                    status,
                                    response_ms,
                                    issues,
                                })
                            }
                        }
                    });
                    handles.push(handle);
                }

                let results = ctx.wait_all(handles).await?;
                let mut service_results = vec![];
                for result in results {
                    let svc = result.map_err(|e| zart::error::TaskError::StepFailed {
                        step: "parallel-health-check".to_string(),
                        source: e,
                    })?;
                    service_results.push(svc);
                }

                let total_issues: usize = service_results.iter().map(|s| s.issues.len()).sum();

                Ok(ParallelOutput {
                    services_checked: service_results.len(),
                    total_issues,
                    results: service_results,
                })
            }
        }

        #[tokio::test]
        #[ignore = "requires PostgreSQL — run with: just test-examples"]
        async fn parallel_example_completes_all_steps() {
            let scheduler = setup().await;

            let mut registry = TaskRegistry::new();
            registry.register("parallel-example", ParallelTask);
            let registry = Arc::new(registry);

            let execution_id = format!("test-parallel-{}", Uuid::new_v4());
            let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

            let input = ParallelInput {
                services: vec![
                    "auth-api".to_string(),
                    "payments".to_string(),
                    "users-db".to_string(),
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
            let result: ParallelOutput =
                serde_json::from_value(record.result.expect("expected result"))
                    .expect("deserialize failed");
            assert_eq!(result.services_checked, 3);
            assert_eq!(result.total_issues, 1);
        }
    }

    // ── Example 4: Radkit Agent ───────────────────────────────────────────────

    mod radkit_agent {
        use super::*;

        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        struct AgentInput {
            query: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct ExtractedLocation {
            city: String,
            state: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct BreweryInfo {
            name: String,
            brewery_type: String,
            city: String,
            state: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct AgentOutput {
            query: String,
            location: ExtractedLocation,
            breweries: Vec<BreweryInfo>,
            summary: String,
            completed_at: String,
        }

        #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
        struct BreweryRaw {
            name: String,
            #[serde(default)]
            brewery_type: Option<String>,
            city: Option<String>,
            #[serde(default)]
            state: Option<String>,
        }

        struct RadkitAgentTask;

        #[async_trait::async_trait]
        impl zart::registry::TaskHandler for RadkitAgentTask {
            type Data = AgentInput;
            type Output = AgentOutput;

            async fn run(
                &self,
                ctx: &mut zart::context::TaskContext,
                data: Self::Data,
            ) -> Result<Self::Output, zart::error::TaskError> {
                // For testing, we skip the LLM extraction and use hardcoded values
                // In production, this would use radkit's LlmFunction

                // Step 1: Extract location (simulated for test - in real code uses LLM)
                let location = ctx
                    .step_with_retry(
                        "extract-location",
                        zart::RetryConfig::exponential(3, Duration::from_millis(100)),
                        || {
                            let query = data.query.clone();
                            async move {
                                // Simulate LLM extraction with simple parsing
                                // In real implementation, this calls radkit's LLM
                                let (city, state) = if query.contains("Portland") {
                                    ("Portland".to_string(), "Oregon".to_string())
                                } else if query.contains("Asheville") {
                                    ("Asheville".to_string(), "North Carolina".to_string())
                                } else {
                                    ("Unknown".to_string(), "Unknown".to_string())
                                };
                                Ok(ExtractedLocation { city, state })
                            }
                        },
                    )
                    .await?;

                // Step 2: Find breweries via external API
                let raw: Vec<BreweryRaw> = ctx
                    .step_with_retry(
                        "find-breweries",
                        zart::RetryConfig::exponential(3, Duration::from_millis(100)),
                        || {
                            let city = location.city.clone();
                            async move {
                                let client = reqwest::Client::new();
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
                    .step("transform-results", || {
                        let raw = raw.clone();
                        let city = location.city.clone();
                        let state = location.state.clone();
                        async move {
                            Ok(raw
                                .into_iter()
                                .map(|b| BreweryInfo {
                                    name: b.name,
                                    brewery_type: b
                                        .brewery_type
                                        .unwrap_or_else(|| "unknown".to_string()),
                                    city: b.city.unwrap_or_else(|| city.clone()),
                                    state: b.state.unwrap_or_else(|| state.clone()),
                                })
                                .collect())
                        }
                    })
                    .await?;

                // Step 4: Generate summary (simulated for test - in real code uses LLM)
                let summary = ctx
                    .step_with_retry(
                        "generate-summary",
                        zart::RetryConfig::exponential(3, Duration::from_millis(100)),
                        || {
                            let location = location.clone();
                            let breweries = breweries.clone();
                            async move {
                                // Simulated summary for testing
                                Ok(format!(
                                    "Found {} breweries in {}, {}!",
                                    breweries.len(),
                                    location.city,
                                    location.state
                                ))
                            }
                        },
                    )
                    .await?;

                Ok(AgentOutput {
                    query: data.query,
                    location,
                    breweries,
                    summary,
                    completed_at: chrono::Utc::now().to_rfc3339(),
                })
            }
        }

        #[tokio::test]
        #[ignore = "requires PostgreSQL and internet — run with: just test-examples"]
        async fn radkit_agent_completes_successfully() {
            let scheduler = setup().await;

            let mut registry = TaskRegistry::new();
            registry.register("radkit-agent-test", RadkitAgentTask);
            let registry = Arc::new(registry);

            let execution_id = format!("test-radkit-{}", Uuid::new_v4());
            let durable = DurableScheduler::new(scheduler.clone(), registry.clone());

            let input = AgentInput {
                query: "Find breweries in Portland".to_string(),
            };
            durable
                .start_typed(&execution_id, "radkit-agent-test", &input)
                .await
                .expect("start failed");

            let worker = spawn_worker(scheduler.clone(), registry);

            let record = durable
                .wait(&execution_id, Duration::from_secs(60), None)
                .await
                .expect("wait failed");

            worker.stop();

            assert_eq!(record.status, ExecutionStatus::Completed);
            let result: AgentOutput =
                serde_json::from_value(record.result.expect("expected result"))
                    .expect("deserialize failed");
            assert!(result.query.contains("Portland"));
            assert_eq!(result.location.city, "Portland");
            assert!(!result.breweries.is_empty());
            assert!(!result.summary.is_empty());
        }
    }
}

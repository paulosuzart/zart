use zart::DurableScheduler;
use zart::admin::RerunSpec;
use zart_scheduler::ExecutionStatus;

use crate::fmt::{fmt_lower, fmt_opt};

pub async fn retry_step(
    durable: DurableScheduler,
    execution_id: String,
    step_name: String,
    triggered_by: Option<String>,
) {
    let run_id = get_run_id_or_exit(&durable, &execution_id).await;
    let task_id = durable
        .retry_step(&run_id, &step_name, triggered_by.as_deref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    println!("Step '{step_name}' retried (new task: {task_id}).");
}

pub async fn restart(
    durable: DurableScheduler,
    execution_id: String,
    payload: Option<String>,
    triggered_by: Option<String>,
) {
    let new_payload = payload.map(|p| {
        serde_json::from_str(&p).unwrap_or_else(|e| {
            eprintln!("error: invalid JSON payload: {e}");
            std::process::exit(1);
        })
    });
    let new_run_id = durable
        .restart(&execution_id, new_payload, triggered_by.as_deref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    println!("Execution '{execution_id}' restarted (new run: {new_run_id}).");
}

pub async fn rerun(
    durable: DurableScheduler,
    execution_id: String,
    rerun: Vec<String>,
    preserve: Vec<String>,
    triggered_by: Option<String>,
) {
    let spec = RerunSpec {
        force_rerun: rerun,
        preserve,
        triggered_by,
    };
    let result = durable
        .rerun_steps(&execution_id, spec)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    println!("New run number: {}", result.new_run_number);
    println!("Steps to rerun: {}", result.effective_rerun.join(", "));
}

pub async fn runs(durable: DurableScheduler, execution_id: String) {
    let runs = durable.list_runs(&execution_id).await.unwrap_or_else(|e| {
        eprintln!("error: {e}");
        std::process::exit(1);
    });
    if runs.is_empty() {
        eprintln!("No runs found for execution '{execution_id}'.");
        std::process::exit(1);
    }
    println!("Runs for execution '{execution_id}':");
    for r in &runs {
        let marker = if r.status == ExecutionStatus::Completed {
            "✓"
        } else {
            ""
        };
        println!(
            "  run:{}  status:{}  started:{}  trigger:{}",
            r.run_index,
            r.status,
            r.started_at,
            fmt_lower(&r.trigger),
        );
        if let Some(result) = &r.result {
            println!("    result: {result}");
        }
        if !marker.is_empty() {
            println!("    {marker}");
        }
    }
}

pub async fn detail(durable: DurableScheduler, execution_id: String, run_id: Option<String>) {
    let detail = durable
        .execution_detail(&execution_id, run_id.as_deref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });

    let ex = &detail.execution;
    println!("Execution : {}", ex.execution_id);
    println!("Task      : {}", ex.task_name);
    println!("Status    : {}", ex.status);
    println!("Scheduled : {}", ex.scheduled_at);
    if let Some(at) = ex.completed_at {
        println!("Completed : {at}");
    }

    if !detail.runs.is_empty() {
        println!("\nRuns:");
        for r in &detail.runs {
            println!(
                "  [{}] run_id:{} trigger:{} status:{} started:{} completed:{}",
                r.run_index,
                r.run_id,
                fmt_lower(&r.trigger),
                r.status,
                r.started_at,
                fmt_opt(r.completed_at),
            );
        }
    }

    if detail.steps.is_empty() {
        println!("\nNo steps recorded for this run.");
    } else {
        println!("\nSteps:");
        for s in &detail.steps {
            let retryable = if s.retryable { " [retryable]" } else { "" };
            println!(
                "  {:<30} kind:{:<12} status:{:<10} attempt:{} completed:{}{}",
                s.step.step_name,
                fmt_lower(&s.step.step_kind),
                fmt_lower(&s.step.status),
                s.step.retry_attempt,
                fmt_opt(s.step.completed_at),
                retryable,
            );
            if let Some(ref err) = s.step.last_error {
                println!("    error: {err}");
            }
            for a in &s.attempts {
                println!(
                    "    attempt {} status:{} started:{} completed:{}",
                    a.attempt_number,
                    fmt_lower(&a.status),
                    a.started_at,
                    fmt_opt(a.completed_at),
                );
                if let Some(ref err) = a.error {
                    println!("      error: {err}");
                }
            }
        }
    }
}

async fn get_run_id_or_exit(durable: &DurableScheduler, execution_id: &str) -> String {
    durable
        .get_current_run_id(execution_id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            eprintln!("error: no run found for execution '{execution_id}'");
            std::process::exit(1);
        })
}

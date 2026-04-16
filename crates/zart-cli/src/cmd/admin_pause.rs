use zart::DurableScheduler;
use zart::admin::PauseScope;
use zart_scheduler::pause_storage::PauseRuleFilter;

pub async fn pause(
    durable: DurableScheduler,
    execution_id: Option<String>,
    task_name: Option<String>,
    step: Option<String>,
    triggered_by: Option<String>,
) {
    let rule = durable
        .pause(PauseScope {
            execution_id,
            task_name,
            step_pattern: step,
            triggered_by,
            ..Default::default()
        })
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    println!("Created pause rule '{}' ({:?}).", rule.rule_id, rule.scope);
}

pub async fn resume(
    durable: DurableScheduler,
    execution_id: Option<String>,
    task_name: Option<String>,
    step: Option<String>,
    triggered_by: Option<String>,
) {
    let result = durable
        .resume(PauseScope {
            execution_id,
            task_name,
            step_pattern: step,
            triggered_by,
            ..Default::default()
        })
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    println!(
        "Resumed: {} pause rule(s) soft-deleted.",
        result.rules_deleted
    );
}

pub async fn delete_pause_rule(
    durable: DurableScheduler,
    rule_id: String,
    triggered_by: Option<String>,
) {
    let deleted = durable
        .resume_rule_by_id(&rule_id, triggered_by.as_deref())
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    if deleted {
        println!("Pause rule '{rule_id}' deleted.");
    } else {
        eprintln!("Pause rule '{rule_id}' not found.");
        std::process::exit(1);
    }
}

pub async fn pause_list(durable: DurableScheduler, include_deleted: bool) {
    let rules = durable
        .list_pause_rules(Some(PauseRuleFilter {
            include_deleted,
            ..Default::default()
        }))
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
    if rules.is_empty() {
        println!("No pause rules found.");
    } else {
        for r in &rules {
            let deleted_marker = if r.deleted_at.is_some() {
                " [DELETED]"
            } else {
                ""
            };
            println!(
                "{} {:?} (created: {}){}",
                r.rule_id, r.scope, r.created_at, deleted_marker
            );
        }
    }
}

# zart-macros

Procedural macros for the [Zart](https://www.zart.run/) durable execution framework.

These macros are **optional** — the `zart` free-function API works without them. They cut boilerplate when defining handlers and steps.

## Macros

| Macro | Purpose |
|-------|---------|
| `#[zart_durable("name")]` | Turns an async function into a durable handler struct implementing `DurableExecution` |
| `#[zart_step("name")]` | Turns an async function into a step-builder struct implementing `ZartStep` |
| `z_wait_event!` | Pause execution until a named external event arrives |
| `zart_capture!` | Durably capture a synchronous value |

## Example

```rust
use zart::prelude::*;

#[zart_durable("send-report", timeout = "10m")]
async fn send_report(data: ReportRequest) -> Result<(), MyError> {
    zart::require(FetchData { id: data.id }).await?;
    zart::require(RenderPdf { id: data.id }).await?;
    zart::require(EmailReport { to: data.email }).await?;
    Ok(())
}
```

`zart-macros` is re-exported by the `zart` crate — you typically don't need to add it as a direct dependency.

## Learn more

- Website: <https://www.zart.run/>
- Repository: <https://github.com/paulosuzart/zart>

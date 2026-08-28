---
name: browser
description: Attach to an already-running Chrome instance through an explicit loopback CDP endpoint, inspect its existing tabs, and perform user-requested page actions from IPython. Use only when Chrome was started separately with remote debugging enabled; this skill never launches or terminates Chrome.
---

# Browser

Attach to an already-running Chrome instance through its explicit loopback
Chrome DevTools Protocol (CDP) endpoint. This skill does not launch Chrome,
discover processes or profiles, or terminate the user's browser.

## Setup

Chrome must already be running with a remote debugging TCP endpoint and a
non-default persistent user-data directory. Chrome 136 and newer ignore remote
debugging switches for the default profile.

Ask the user for the endpoint when it is not already supplied. It must have the
form `http://127.0.0.1:<port>`. Do not search for it with shell commands,
AppleEvents, Unix sockets, or process inspection.

## Usage

Always use an async context manager so cancellation and errors close only the
skill-owned HTTP and WebSocket connections:

```python
async with await browser.connect("http://127.0.0.1:9222") as chrome:
    print(await chrome.targets())
    async with await chrome.page(url_contains="example.com") as page:
        print(await page.read_text("body"))
```

Choose a target explicitly when more than one page is open. A unique
`target_id`, `url_contains`, or `title_contains` selector is supported.

Page operations use the same enclosing IPython cell authority:

```python
async with await browser.connect("http://127.0.0.1:9222") as chrome:
    async with await chrome.page(title_contains="Settings") as page:
        previous = await page.fill("#display-name", "New name")
        print(previous)
        await page.click("button[type=submit]")
```

Use `page.evaluate(...)` only when the narrower `read_text`, `fill`, `click`,
or `navigate` methods are insufficient. One approved cell may contain several
browser operations. Never claim a page was read or changed when attachment,
target selection, navigation, or evaluation failed.

## Lifecycle and limitations

- `browser.close()` and `page.close()` close only client connections. They
  never close a Chrome tab or browser process.
- Existing authentication and tab state belong to the Chrome profile. The
  skill does not read profile files or store credentials.
- Concurrent clients, target ownership, profile setup, site compatibility,
  login, MFA, CAPTCHA, downloads, and uploads remain caller-managed.
- CDP behavior varies by Chrome version. An unavailable endpoint, ambiguous
  target, disconnected tab, protocol error, or inaccessible site is reported
  as an ordinary browser limitation.

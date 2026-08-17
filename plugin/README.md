# Tincan Inbox herdr plugin

`tincan` must be available on the herdr server's `PATH`. Link this checkout's plugin directory:

```sh
herdr plugin link ./plugin
```

Add this command keybinding to `~/.config/herdr/config.toml`:

```toml
[[keys.command]]
key = "prefix+t"
type = "plugin_action"
command = "tincan.inbox.open-inbox"
description = "tincan inbox"
```

The inbox opens as a popup and leaves the existing pane layout and composer intact. If an action or pane fails, inspect `herdr plugin log list` for its command output.

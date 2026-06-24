# MailHog Link Plugin

This plugin adds a direct Airflow UI link to the MailHog web interface used by the local Cosiflow stack.

## Menu Entry

The plugin registers the following Airflow menu item:

```text
Develop tools -> Mailhog
```

The local Airflow route is:

```text
/mailhog
```

## Target URL

The redirect target is read from:

```text
MAILHOG_WEBUI_URL
```

If the environment variable is not set, it falls back to:

```text
http://localhost:8025
```

## Structure

```text
mailhog_link/
├── mailhog_link_plugin.py
├── mailhog_link_view_plugin.py
└── README.md
```

## Notes

This plugin only exposes a convenient UI link. Mail delivery and SMTP capture are configured in the Cosiflow Airflow environment.

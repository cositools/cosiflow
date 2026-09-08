# MailHog Link Plugin

This plugin adds a direct Airflow UI link to the MailHog web interface used by the local Cosiflow stack.

## Menu Entry

The plugin registers the following Airflow menu item:

```text
Develop Tools -> Mailhog
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
├── mailhog_link_view_plugin.py
└── README.md
```

## Notes

The view requires `can_read` on `COSIflow Mail Sandbox`; only Operator and Admin
receive that permission and menu entry. This plugin only exposes a
convenient UI redirect; it does not add links to individual failed tasks.
Mail delivery and SMTP capture are configured in the COSIflow environment.
The permission does not protect direct access to MailHog, so its host ports
must remain bound to loopback in local development and unpublished in shared
deployments.

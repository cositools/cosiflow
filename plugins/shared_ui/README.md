# Shared plugin UI

This directory contains Jinja and CSS resources shared by the COSIflow Airflow
plugins.

## Use from a plugin

Add the shared loader to the plugin Blueprint:

```python
from shared_ui import add_shared_templates

plugin_blueprint = add_shared_templates(
    Blueprint(
        "plugin_blueprint",
        __name__,
        template_folder=os.path.join(plugin_folder, "templates"),
    )
)
```

Then import the shared styles from a template:

```jinja
{% include "cosiflow_ui/styles.html" %}
```

The common page wrapper is:

```html
<div class="cosiflow-page">
  ...
</div>
```

Add `cosiflow-page--contained` for a centered maximum width. A plugin can
customize only the relevant values instead of copying the common rules:

```css
.my-plugin-page {
  --cosiflow-page-padding-block: 24px;
  --cosiflow-page-max-width: 1440px;
}
```

Use `cosiflow-page--flush` when a page intentionally needs no outer spacing.

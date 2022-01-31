# NewRelicUploader

Taurus plugin to stream results to NewRelic API

## Installation

```bash
git clone https://github.com/doctornkz/newrelicUploader.git
cd newrelicUploader/
pip install .
```

## Example of configuration file

```ini
execution:
- executor: pbench
  concurrency: 10
  hold-for: 10m
  ramp-up: 2m
  #iterations: 100
  throughput: 10
  scenario:
    requests:
      - url: https://example.com/
        label: pbench

reporting:
  - module: newrelic

modules:
    console:
      disable: true
      screen: console

    newrelic:
      dashboard-url: https://onenr.io/PLACEHOLDER
      project: my_project
      browser-open: none  # auto-open the report in browser, 
                            # can be "start", "end", "both", "none"
      send-interval: 5s   # send data each n-th second
      # token-file: token.txt
      # custom-tags:
      #   example: '1'

```

## Authentification

You can use:

- `NEW_RELIC_INSERT_KEY` environment variable to pass the token
- `token` from configuration file (see example above)
- String from file `token-file` (see example above)
If it's failing, test will be intterupted.

## Starting

```bash
UNDERCONSTRUCTION
```

## Dashboard example

TBC
# JIRA issues converter

## Getting started

1. Define a file with name `input.csv`
2. Define a file with name `decoder.yml`

```yml
sprints:
  - input_key: "Sprint-2024-05-S5"
    output_key: "LYNX-2024-05-S5"
  - input_key: "Sprint-2024-06-S1"
    output_key: "2024-06-S1"
epics:
  - input_key: "DEVFELEAS-227"
    output_key: "BIN-6"
  - input_key: "DEVFELEAS-223"
    output_key: "BIN-4"
```

3. Run `./jira`

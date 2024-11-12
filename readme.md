# What is this?

This a simple tool to gather and get insights into the software development process.

⚠️ **WARNING!** Measuring software development is a complex and controversial topic.
However, we naturally accumulate a lot of data, and it is beautiful and precious.
It can be used to gather insights when used with extreme care and due diligence.

![image](https://github.com/user-attachments/assets/8f95611d-3068-49da-9e32-93a027b05bb1)

# How to use it?

1. Create config YAML file
2. Create virtual environment for dependencies `python -m venv .venv && source .venv/bin/activate`
3. Install requirements `python -m pip install -r requirements.txt`
4. Run `codoscope --config-path config.yaml process`

# Prerequsites

* Python >= 3.12

## Sample configuration file

The simplest config file that allows to ingest a repository and build some simple reports is below.

```yaml
state-path: state.codoscope

ingestion:
  enabled: true
  sources:
    - name: "my repo"
      type: "git"
      enabled: true
      path: ~/src/repo

reports:
  - name: overview
    type: overview
    timezone: "Europe/Amsterdam"
    out-path: reports/overview.html

  - name: per-source-stats
    type: per-source-stats
    timezone: utc
    out-dir: reports/per-source-stats

  - name: per-user-stats
    type: per-user-stats
    timezone: utc
    out-dir: reports/per-user-stats

  - name: pr-reviews
    type: pr-reviews
    ignored-users:
      - Build Bot
    out-path: reports/pr-reviews.html

  - name: word-clouds
    type: word-clouds
    grouping-period: Q
    out-path: reports/word-clouds.html
```

For more examples refer to sample [config.yaml](config.yaml).

## Processors

### Users remapping

Frequently users use different emails/names and in order to group it data needs to be remapped.
Below is sample snippet how to engage it.

```yaml
processors:
  - name: remap-users
    type: remap-users
    canonical-names:
      John Smith:
      - email: john@john.smith.com
      - email: john@organization.com
      - name: John Sr Smith
```

# Supported sources

* Git repositories
* BitBucket workspaces
  * Pull requests and comments
* JIRA
  * Issues
  * Comments

# Flexible reports

Powered by [plotly](https://github.com/plotly/plotly.py) it allows to present hundreds of thousands of data points in a single interactive chart with relative ease.

### Reports samples

![sample-overview](https://github.com/user-attachments/assets/36a76223-7bea-4b50-bd50-1cfb2ec04746)

![sample-overview-2](https://github.com/user-attachments/assets/aeadbc3d-13aa-4026-9422-ebd1dbc01040)

Based on private repository however the data is anonymized, user names are randomly generated.

https://github.com/user-attachments/assets/f82079a8-114b-44ef-9552-2155fea22096

The below is based on public Apache Lucene repository (https://github.com/apache/lucene).

![sample-line-counts](https://github.com/user-attachments/assets/d58d4e9e-b80a-451c-a7d7-2a8674285fcf)

![sample-github-style](https://github.com/user-attachments/assets/c1b38ce9-faec-4431-8843-7e8284676c0b)

![sample-code-map](https://github.com/user-attachments/assets/00f07ebd-ceee-4e07-9f97-c3f3760904da)

![sample-week-day-stats](https://github.com/user-attachments/assets/5558f704-0768-4f09-97cf-0314b19748e3)
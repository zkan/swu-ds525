# Analytics Engineering

`~/.dbt/profiles.yml`

```yml
greenery:
  outputs:

    dev:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: greenery
      schema: public

    prod:
      type: postgres
      threads: 1
      host: localhost
      port: 5432
      user: postgres
      pass: postgres
      dbname: greenery
      schema: prod

  target: dev
```

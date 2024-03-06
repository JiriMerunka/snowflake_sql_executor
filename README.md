# Snowflake SQL executor

# Snowflake SQL executor

- connects to Snowflake db instance
- generates SQL (DL) by using Liquid Template language + config files in JSON (1 global = config.json + multiple in param_definitions dir - config for each table) and executes them in Snowflake

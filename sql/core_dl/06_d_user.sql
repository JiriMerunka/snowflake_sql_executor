--insert transformation sql here

--run templates
#{date_to_scd2(param_definition_file=wrk_d_user.json)}
#{scd2_core(param_definition_file=d_user.json)}
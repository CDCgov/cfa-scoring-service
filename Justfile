score predicted report_date final output:
    Rscript -e "devtools::load_all(); \
    local_path_final <- cfascoring::download_if_specified(\
        '{{final}}',\
        'nssp-etl',\
        'tmp'\
    ); \
    local_path_predicted <- cfascoring::download_if_specified(\
        '{{predicted}}',\
        'zs-test-pipeline-update',\
        'tmp'\
    ); \
    local_output_path <- cfascoring::write_scores(\
        local_path_predicted,\
        '{{report_date}}',\
        local_path_final,\
        '{{output}}'\
    );\
    cfascoring::upload_to_container('{{output}}', 'scoring-test-output');"

score predicted report_date final output:
    Rscript -e "devtools::load_all(); \
    local_path_final <- cfascoring::download_if_specified(\
        'gold/{{final}}.parquet',\
        'nssp-etl',\
        'tmp'\
    );"


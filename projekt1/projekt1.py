from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime

with DAG(
    "project1-workflow",
    start_date=datetime(2015, 12, 1),
    schedule_interval=None,
    # TODO Uruchamiając projekt, za każdym razem w konfiguracji uruchomienia Apache Airflow popraw ścieżki w parametrach dags_home i input_dir
    # TODO Zmień poniżej domyślne wartości parametrów classic_or_streaming oraz pig_or_hive na zgodne z Twoim projektem
    params={
        "dags_home": Param("/home/TU WPISZ NAZWĘ TWOJEGO KATALOGU/airflow/dags", type="string"),
        "input_dir": Param("gs://TU WPISZ NAZWĘ TWOJEGO ZASOBNIKA/projekt1/input", type="string"),
        "output_mr_dir": Param("/project1/output_mr3", type="string"),
        "output_dir": Param("/project1/output6", type="string"),
        "classic_or_streaming": Param("streaming", enum=["classic", "streaming"]),
        "pig_or_hive": Param("pig", enum=["hive", "pig"]),
    },
    render_template_as_native_obj=True
) as dag:
    clean_output_mr_dir = BashOperator(
        task_id="clean_output_mr_dir",
        bash_command="""if $(hadoop fs -test -d {{ params.output_mr_dir }}) ; then hadoop fs -rm -f -r {{ params.output_mr_dir }}; fi""",
    )

clean_output_dir = BashOperator(
    task_id="clean_output_dir",
    bash_command="""if $(hadoop fs -test -d {{ params.output_dir }}) ; then hadoop fs -rm -f -r {{ params.output_dir }}; fi""",
)


def _pick_classic_or_streaming():
    if dag.params['classic_or_streaming'] == "classic":
        return "mapreduce_classic"
    else:
        return "hadoop_streaming"


pick_classic_or_streaming = BranchPythonOperator(
    task_id="pick_classic_or_streaming", python_callable=_pick_classic_or_streaming
)

# TODO Jeśli w Twoim projekcie wykorzystywany jest MR Classic zmień poniższe polecenie dostosowując sposób uruchamiania zadania MR
mapreduce_classic = BashOperator(
    task_id="mapreduce_classic",
    bash_command="""""",
)

# TODO Jeśli w Twoim projekcie wykorzystywany jest Hadoop Streaming zmień poniższe polecenie dostosowując sposób uruchamiania zadania MR
hadoop_streaming = BashOperator(
    task_id="hadoop_streaming",
    bash_command="""mapred streaming \
-D stream.num.map.output.key.fields=2 \
-files {{ params.dags_home }}/project_files/mapper.py,\
{{ params.dags_home }}/project_files/combiner.py,\
{{ params.dags_home }}/project_files/reducer.py \
-input {{ params.input_dir }}/datasource1 \
-mapper  mapper.py \
-combiner combiner.py \
-reducer reducer.py \
-output {{ params.output_mr_dir }}
hadoop fs -getmerge {{ params.output_mr_dir }} results.tsv
hadoop fs -copyFromLocal results.tsv {{params.output_mr_dir}}""",
)


def _pick_pig_or_hive():
    if dag.params['pig_or_hive'] == "pig":
        return "pig"
    else:
        return "hive"


pick_pig_or_hive = BranchPythonOperator(
    task_id="pick_pig_or_hive", python_callable=_pick_pig_or_hive, trigger_rule="none_failed",
)

#  TODO Jeśli w Twoim projekcie wykorzystywany jest Hive, zmień poniższe polecenia dostosowując sposób uruchamiania skryptu Hive
hive = BashOperator(
    task_id="hive",
    bash_command="""hive -f {{ params.dags_home }}/project_files/transform5.hql \
-hiveconf input_dir4={{ params.input_dir }}/datasource4 \
-hiveconf input_dir3={{ params.output_mr_dir }} \
-hiveconf output_dir6={{ params.output_dir }}""",
)

# TODO Jeśli w Twoim projekcie wykorzystywany jest Pig, zmień poniższe polecenia dostosowując sposób uruchamiania skryptu Pig
pig = BashOperator(
    task_id="pig",
    bash_command="""export PIG_CLASSPATH=/etc/hadoop/conf.empty:/etc/tez/conf
pig -x tez -f {{ params.dags_home }}/project_files/transform5.pig \
-param input_dir4={{ params.input_dir }}/datasource4 \
-param input_dir3={{ params.output_mr_dir }} \
-param output_dir6={{ params.output_dir }}""",
)

get_output = BashOperator(
    task_id="get_output",
    bash_command="""hadoop fs -getmerge {{ params.output_dir }} output6.json
cat output6.json""",
    trigger_rule="none_failed",
)

[clean_output_mr_dir, clean_output_dir] >> pick_classic_or_streaming
pick_classic_or_streaming >> [mapreduce_classic, hadoop_streaming]
[mapreduce_classic, hadoop_streaming] >> pick_pig_or_hive
pick_pig_or_hive >> [pig, hive]
[pig, hive] >> get_output

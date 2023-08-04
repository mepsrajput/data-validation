from google.cloud import bigquery

project_id = "your-project-id"
dataset_id = "your-dataset-id"
table1_name = "table1_name"
table2_name = "table2_name"
base_variable = "your_numeric_variable_name"
bucket_count = 20

def create_bucket_query(bucket_count, min_value, max_value):
    step = (max_value - min_value) / bucket_count
    buckets = [min_value + i * step for i in range(bucket_count)]
    bucket_ranges = zip(buckets, buckets[1:] + [max_value])
    bucket_query = 'CASE '
    for i, (start, end) in enumerate(bucket_ranges):
        bucket_query += f"WHEN {base_variable} >= {start} AND {base_variable} <= {end} THEN {i} "
    bucket_query += f"ELSE {bucket_count} END AS bucket"
    return bucket_query

from google.cloud import bigquery

project_id = "your-project-id"
dataset_id = "your-dataset-id"
table1_name = "table1_name"
table2_name = "table2_name"
base_variable = "your_numeric_variable_name"
bucket_count = 20

def create_bucket_query(bucket_count, min_value, max_value):
    # Same as before

def calculate_bucket_counts(client, dataset_id, table1_name, table2_name, base_variable, bucket_count):
    # Calculate min and max values from the data
    query = f"""
        SELECT
            MIN({base_variable}) AS min_value,
            MAX({base_variable}) AS max_value
        FROM
            `{project_id}.{dataset_id}.{table1_name}`
        UNION ALL
        SELECT
            MIN({base_variable}) AS min_value,
            MAX({base_variable}) AS max_value
        FROM
            `{project_id}.{dataset_id}.{table2_name}`
    """

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        min_value = row['min_value']
        max_value = row['max_value']

    bucket_query = create_bucket_query(bucket_count, min_value, max_value)

    query = f"""
        WITH
        t1 AS (
            SELECT {base_variable}, {bucket_query} FROM `{project_id}.{dataset_id}.{table1_name}`
        ),
        t2 AS (
            SELECT {base_variable}, {bucket_query} FROM `{project_id}.{dataset_id}.{table2_name}`
        )
        SELECT
            bucket,
            COUNT(t1.{base_variable}) as count_t1,
            COUNT(t2.{base_variable}) as count_t2
        FROM
            t1
        FULL OUTER JOIN
            t2
        USING (bucket)
        GROUP BY
            bucket
        ORDER BY
            bucket
    """

client = bigquery.Client(project=project_id)
bucket_query = create_bucket_query(bucket_count, 0, 1)
bucket_counts = calculate_bucket_counts(client, dataset_id, table1_name, table2_name, base_variable, bucket_count)

# Create a DataFrame with the final output
data = {
    'Bucket': [],
    'Table1_Count': [],
    'Table2_Count': []
}

for bucket_info in bucket_counts:
    bucket_str = f"({bucket_info['min_value']:.2f}-{bucket_info['max_value']:.2f})"
    count_t1 = bucket_info['count_t1']
    count_t2 = bucket_info['count_t2']
    
    data['Bucket'].append(bucket_str)
    data['Table1_Count'].append(count_t1)
    data['Table2_Count'].append(count_t2)

df = pd.DataFrame(data)
print(df)

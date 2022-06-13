from pyspark.sql.functions import col, trunc, to_date
from pyspark.sql.types import DateType

def clean_data_columns_pyspark(dataset, date_columns=['Month']):
    """
        Returns revenue data with timestamps in revenue_data[date_column] set as datetime objects and normalised to
        have the first day of each month.
            Args:
                dataset (Spark DataFrame): dataset with some date columns.
                date_columns (str): titles for columns with payment dates currently datetime objects or standard
                                    format datetime strings.
            Returns:
                revenue_data_clean (Spark DataFrame): dateset with date columns set to datetime objects.
    """

    # cast datatype of entries to date columns as PySpark DateType objects
    dataset_dt = dataset.select(*(to_date(col(c)).alias(c) for c in date_columns))

    dataset_clean = dataset_dt.select(*(trunc(col(c), "Month").alias(c) for c in date_columns))

    return dataset_clean
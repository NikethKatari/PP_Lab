# Databricks notebook source
def dropNa(dataframe):
    try:
        df_dropped = dataframe.na.drop()
        return df_dropped
    except Exception as e:
        print(f"Error in dropNa: {e}")
        return dataframe


# COMMAND ----------

def dropNaIfAll(dataframe):
    try:
        df_droppedall = dataframe.na.drop(how="all")
        return df_droppedall
    except Exception as e:
        print(f"Error in dropNaIfAll: {e}")
        return dataframe


# COMMAND ----------

def fillNaWithZero(dataframe):
    try:
        df_zerofilled = dataframe.na.fill(value=0)
        return df_zerofilled
    except Exception as e:
        print(f"Error in fillNaWithZero: {e}")
        return dataframe


# COMMAND ----------

def DropDuplicates(dataframe):
    try:
        df_dropped = dataframe.dropDuplicates()
        return df_dropped
    except Exception as e:
        print(f"Error in DropDuplicates: {e}")
        return dataframe


# COMMAND ----------

def FilterOnGreaterThan(dataframe, column, value):
    try:
        if column not in dataframe.columns:
            raise ValueError(f"Column '{column}' does not exist in DataFrame.")
        df_filtered = dataframe.filter(col(column) > value)
        return df_filtered
    except Exception as e:
        print(f"Error in FilterOnGreaterThan: {e}")
        return dataframe


# COMMAND ----------

def FilterOnLesserThan(dataframe, column, value):
    try:
        if column not in dataframe.columns:
            raise ValueError(f"Column '{column}' does not exist in DataFrame.")
        df_filtered = dataframe.filter(col(column) < value)
        return df_filtered
    except Exception as e:
        print(f"Error in FilterOnLesserThan: {e}")
        return dataframe


# COMMAND ----------

def RenameColumn(dataframe, oldname, newname):
    try:
        if oldname not in dataframe.columns:
            raise ValueError(f"Column '{oldname}' does not exist in DataFrame.")
        df_renamed = dataframe.withColumnRenamed(oldname, newname)
        return df_renamed
    except Exception as e:
        print(f"Error in RenameColumn: {e}")
        return dataframe


# COMMAND ----------

def calculate_average_of_col(dataframe, column):
    try:
        if column not in dataframe.columns:
            raise ValueError(f"Column '{column}' does not exist in DataFrame.")
        mean_col = dataframe.select(mean(col(column))).collect()[0][0]
        return float(mean_col)  # Returning as float instead of int
    except Exception as e:
        print(f"Error in calculate_average_of_col: {e}")
        return None

# COMMAND ----------

def TrimSpaces(dataframe,column):
    try:
        if column not in dataframe.columns:
            raise ValueError(f"Column '{column}' does not exist in DataFrame.")
        df_trimmed = dataframe.withColumn(column, trim(dataframe[column]))
        return df_trimmed
    except Exception as e:
        print(f"Error in TrimSpaces: {e}")
        return dataframe

# COMMAND ----------

def ChangeCase(dataframe, column, case):
    try:
        if column not in dataframe.columns:
            raise ValueError(f"Column '{column}' does not exist in DataFrame.")
        if case == 'lower':
            return dataframe.withColumn(column, lower(dataframe[column]))
        elif case == 'upper':
            return dataframe.withColumn(column, upper(dataframe[column]))
        else:
            raise ValueError("Invalid case specified. Use 'lower' or 'upper'.")
    except Exception as e:
        print(f"Error in ChangeCase: {e}")
        return dataframe

# COMMAND ----------

data = [
    (1, "Alice", None),
    (2, "Bob", 10),
    (3, "CHARLIE", None),
    (4, None, 20),
    (5, "Diana", 30)
]
df = spark.createDataFrame(data, ["id", "name", "value"])

# COMMAND ----------

print("Original DataFrame:")
display(df)

print("DataFrame after dropNa:")
df_dropped = dropNa(df)
display(df_dropped)

print("DataFrame after dropNaIfAll:")
df_droppedall = dropNaIfAll(df)
display(df_droppedall)

print("DataFrame after fillNaWithZero:")
df_zerofilled = fillNaWithZero(df)
display(df_zerofilled)

print("DataFrame after DropDuplicates:")
df_dropped_duplicates = DropDuplicates(df)
display(df_dropped_duplicates)

print("DataFrame filtered where 'value' > 15:")
df_filtered_gt = FilterOnGreaterThan(df, "value", 15)
display(df_filtered_gt)

print("DataFrame filtered where 'value' < 25:")
df_filtered_lt = FilterOnLesserThan(df, "value", 25)
display(df_filtered_lt)

print("DataFrame after renaming 'name' to 'full_name':")
df_renamed = RenameColumn(df, "name", "full_name")
display(df_renamed)

print("Average of 'value' column:")
average_value = calculate_average_of_col(df, "value")
print(average_value)

print("DataFrame after trimming spaces from 'full_name':")
df_trimmed = TrimSpaces(df_renamed, "trimmed_name", "full_name")
display(df_trimmed)

print("DataFrame after converting 'full_name' to lowercase:")
df_lower = ChangeCase(df_trimmed, "trimmed_name", "lower")
display(df_lower)

print("DataFrame after converting 'full_name' to uppercase:")
df_upper = ChangeCase(df_trimmed, "trimmed_name", "upper")
display(df_upper)

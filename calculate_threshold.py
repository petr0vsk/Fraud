####################################################################################################################################################################################################################
# Эта программа выполняет кластеризацию юридических лиц на основе их платежной активности (90-й процентиль и общий объем платежей), оценивает качество кластеризации с помощью силуэтного коэффициента, 
# рассчитывает пороги срабатывания для системы антифрода и выявляет ложные срабатывания на основании транзакций, превышающих порог в каждом кластере. Программа также подсчитывает общее количество транзакций 
# и ложных срабатываний для каждого юридического лица, а результаты сохраняются в Excel-файлы с добавленными столбцами для количества транзакций и ошибок (ложных срабатываний) для каждого кластера.
#####################################################################################################################################################################################################################3

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# 1. Загрузка и подготовка данных

def load_and_process_csv(csv_file):
    """
    Загружает данные из CSV, проверяет их корректность и выполняет необходимые преобразования.

    Аргументы:
        csv_file (str): путь к CSV-файлу.

    Возвращает:
        pd.DataFrame: обработанный датафрейм с рассчитанными процентилями и оборотами.
    """
    df = pd.read_csv(csv_file, delimiter=';', header=None)
    df.columns = ['inn', 'org_name', 'amount', 'type', 'code', 'timestamp']

    # Преобразование суммы в числовой формат
    df['amount'] = df['amount'].apply(lambda x: str(x).replace(',', '.'))
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    # Группировка по INN и расчет percentil 90 и total_payments
    result = df.groupby(['inn', 'org_name']).agg(
        percentile_90=('amount', lambda x: x.quantile(0.9)),
        total_payments=('amount', 'sum')
    ).reset_index()

    return df, result  # Возвращаем и исходный DataFrame для расчетов ложных срабатываний

# 2. Кластеризация с тюнингом параметров

def cluster_businesses_with_tuning(df, n_clusters=3, init='k-means++', n_init=10, max_iter=300):
    """
    Выполняет кластеризацию с тюнингом параметров.

    Аргументы:
        df (pd.DataFrame): датафрейм с данными для кластеризации.
        n_clusters (int): количество кластеров.
        init (str): метод инициализации центроидов ('k-means++' или 'random').
        n_init (int): количество запусков алгоритма.
        max_iter (int): максимальное количество итераций.

    Возвращает:
        список датафреймов: датафреймы для каждого кластера.
    """
    clustering_data = df[['percentile_90', 'total_payments']]
    kmeans = KMeans(n_clusters=n_clusters, init=init, n_init=n_init, max_iter=max_iter, random_state=0)
    df['n_cluster'] = kmeans.fit_predict(clustering_data)

    clusters = []
    for i in range(n_clusters):
        clusters.append(df[df['n_cluster'] == i].copy())

    return clusters

# 3. Оценка модели с использованием силуэтного коэффициента

def evaluate_clustering_with_silhouette(df_list):
    """
    Оценивает качество кластеризации с помощью силуэтного коэффициента.

    Аргументы:
        df_list (list): список датафреймов для каждого кластера.

    Возвращает:
        float: силуэтный коэффициент.
    """
    combined_df = pd.concat(df_list)
    X = combined_df[['percentile_90', 'total_payments']]
    labels = combined_df['n_cluster']
    
    score = silhouette_score(X, labels)
    return score

# 4. Расчет порогов срабатывания и сохранение результатов

def calculate_and_save_thresholds_with_names(clustered_dfs, cluster_names):
    """
    Вычисляет пороги для каждого кластера и сохраняет результаты.

    Аргументы:
        clustered_dfs (list): список датафреймов для каждого кластера.
        cluster_names (list): имена для каждого кластера (например, ['small', 'medium', 'big']).

    Возвращает:
        dict: словарь с порогами для каждого кластера.
    """
    thresholds = {}
    
    for i, cluster_df in enumerate(clustered_dfs):
        threshold_value = cluster_df['percentile_90'].quantile(0.9)
        thresholds[cluster_names[i]] = threshold_value

        cluster_df['threshold'] = threshold_value

        # Сохранять промежуточные файлы не нужно, т.к. мы сохраняем итоговые с ошибками
        print(f"Порог для кластера '{cluster_names[i]}': {threshold_value}")
    
    return thresholds

# 5. Расчет ложных срабатываний

def calculate_false_triggers(df_transactions, df_cluster, threshold):
    """
    Рассчитывает количество ложных срабатываний для каждого ИНН в кластере.

    Аргументы:
        df_transactions (pd.DataFrame): датафрейм с транзакциями.
        df_cluster (pd.DataFrame): датафрейм с данными кластера.
        threshold (float): порог для данного кластера.

    Возвращает:
        pd.DataFrame: датафрейм с добавленным столбцом 'errors'.
    """
    # Подсчитываем количество транзакций по каждому ИНН
    transaction_counts = df_transactions.groupby('inn').agg(total_transactions=('amount', 'count')).reset_index()

    merged_df = pd.merge(df_transactions, df_cluster[['inn', 'threshold']], on='inn', how='left')

    # Рассчитываем ложные срабатывания
    merged_df['errors'] = merged_df['amount'] > threshold

    # Считаем количество ложных срабатываний по каждому ИНН
    false_triggers_df = merged_df.groupby('inn').agg(total_errors=('errors', 'sum')).reset_index()

    # Объединяем обратно с кластером и добавляем количество транзакций
    df_cluster = pd.merge(df_cluster, false_triggers_df[['inn', 'total_errors']], on='inn', how='left')
    df_cluster = pd.merge(df_cluster, transaction_counts, on='inn', how='left')

    # Проверяем результат
    print(f"Количество ложных срабатываний и транзакций добавлено для кластера: {df_cluster[['inn', 'total_errors', 'total_transactions']].head()}")

    return df_cluster

# 6. Основная часть кода

if __name__ == "__main__":
    # Загрузка данных
    file_path = 'fraud_02.2024.csv'
    df_transactions, df_aggregated = load_and_process_csv(file_path)

    # Кластеризация на агрегированных данных
    clustered_dfs = cluster_businesses_with_tuning(df_aggregated, n_clusters=3, init='k-means++', n_init=20, max_iter=500)

    # Оценка силуэтного коэффициента
    silhouette_score_value = evaluate_clustering_with_silhouette(clustered_dfs)
    print(f"Силуэтный коэффициент: {silhouette_score_value}")

    # Определение имен для каждого кластера
    cluster_names = ['small', 'medium', 'big']

    # Расчет порогов для каждого кластера и сохранение файлов
    thresholds = calculate_and_save_thresholds_with_names(clustered_dfs, cluster_names)

    # Расчет ложных срабатываний для каждого кластера с использованием исходных данных транзакций
    for i, cluster_name in enumerate(cluster_names):
        threshold = thresholds[cluster_name]
        df_cluster = clustered_dfs[i]

        # Рассчитываем ложные срабатывания
        df_cluster_with_errors = calculate_false_triggers(df_transactions, df_cluster, threshold)

        # Сохраняем данные с ложными срабатываниями и количеством транзакций обратно в файл кластера
        file_name = f"{cluster_name}_cluster_data_with_errors.xlsx"
        df_cluster_with_errors.to_excel(file_name, index=False)
        print(f"Файл для кластера '{cluster_name}' с ложными срабатываниями и количеством транзакций сохранен как {file_name}")

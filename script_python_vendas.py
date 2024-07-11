from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    # Criar uma SparkSession
    spark = SparkSession.builder \
        .appName("Leitura de CSV com PySpark") \
        .getOrCreate()

    # Definir o caminho para o arquivo CSV
    csv_file_path = "vendas.csv"

    # Ler o arquivo CSV
    df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

    # Renomear as colunas para minúsculas e substituir espaços por underscores
    new_column_names = [col.lower().replace(" ", "_") for col in df.columns]
    df_renamed = df.toDF(*new_column_names)

    # 1- Identificar o produto mais vendido em termos de quantidade e canal.
    produto_mais_vendido_por_quantidade_e_canal = (
        df_renamed
            .groupBy('item_type','sales_channel')
            .agg(F.sum('units_sold').alias('total_units_sold'))
            .orderBy(F.desc('total_units_sold'))
            .first()
    )

    # Extrair os resultados
    item_type_mais_vendido = produto_mais_vendido_por_quantidade_e_canal['item_type']
    sales_channel_mais_vendido = produto_mais_vendido_por_quantidade_e_canal['sales_channel']
    total_units_sold_mais_vendido = produto_mais_vendido_por_quantidade_e_canal['total_units_sold']

    # Mostrar o resultado do item mais vendido
    print(f"O produto mais vendido é '{item_type_mais_vendido}' com {total_units_sold_mais_vendido} unidades vendidas pelo canal '{sales_channel_mais_vendido}'.")

    # 2- Determinar qual país e região teve o maior volume de vendas (em valor).
    pais_e_regiao_com_maior_volume_de_vendas = (
        df_renamed
            .groupBy('country','region')
            .agg(F.sum('total_revenue').alias('total_revenue_sum'))
            .orderBy(F.desc('total_revenue_sum'))
            .withColumn('total_revenue_sum', F.format_number('total_revenue_sum',2))
            .first()
    )

    # Extrair os resultados
    region_maior_vendas = pais_e_regiao_com_maior_volume_de_vendas['region']
    country_maior_vendas = pais_e_regiao_com_maior_volume_de_vendas['country']
    total_sales_volume = pais_e_regiao_com_maior_volume_de_vendas['total_revenue_sum']

    # Mostrar o resultado da região e país com maior volume de vendas
    print(f"A região e país com o maior volume de vendas em valor é '{region_maior_vendas}' - '{country_maior_vendas}' com um total de vendas de ${total_sales_volume}")

    # 3- Calcular a média de vendas mensais por produto, considerando o período dos dados disponíveis
    # Converter as colunas de datas para o formato DateType
    # Adicionar colunas de mês e ano para facilitar o agrupamento mensal
    # e calcular média de vendas mensais por produto
    media_de_vendas_mensais_por_produto = (
        df_renamed
            .withColumn('order_date', F.to_date(F.col('order_date'), 'M/d/yyyy'))
            .withColumn('order_month', F.month('order_date'))
            .withColumn('order_year', F.year('order_date'))
            .groupBy('item_type', 'order_month', 'order_year')
            .agg(F.avg('units_sold').alias('media_vendas_mensais'))
    )

    # Mostrar a média de vendas mensais por produto
    print("\n#####################################\n# Média de vendas mensais por produto #\n#####################################\n")
    media_de_vendas_mensais_por_produto.show(50)

    # Parar a SparkSession ao final do processamento
    spark.stop()

if __name__ == "__main__":
    main()

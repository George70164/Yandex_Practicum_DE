# Модификация ETL и витрины.
Цели проекта: модифицировать процессы в пайплайне, чтобы они соответствовали новым задачам бизнеса. Обеспечить обратную совместимость. Создать обновленную витрину данных для исследования возвращаемости клиентов 

## **Постановка задачи**
1. Команда разработки добавила в систему заказов магазина функционал отмены заказов и возврата средств (refunded). Требуется обновить процессы в пайплайне для учета нового функционала: использовать в витрине mart.f_sales статусы shipped и refunded. Все данные в витрине следует считать shipped.

2. Вычислить метрики customer retention в дополнительной витрине.  
Наполнить витрину.  
Эта витрина должна отражать следующую информацию:
* Рассматриваемый период — week.  
* Возвращаемость клиентов: 
    - new — количество клиентов, которые оформили один заказ за рассматриваемый период;
    - returning — количество клиентов, которые оформили более одного заказа за рассматриваемый период;
    - refunded — количество клиентов, которые вернули заказ за рассматриваемый период.
* Доход (revenue) и refunded для каждой категории покупателей.
С помощью новой витрины можно будет выяснить, какие категории товаров лучше всего удерживают клиентов.

Требуемая схема витрины:  
- new_customers_count — кол-во новых клиентов (тех, которые сделали только один 
заказ за рассматриваемый промежуток времени).
- returning_customers_count — кол-во вернувшихся клиентов (тех,
которые сделали только несколько заказов за рассматриваемый промежуток времени).
- refunded_customer_count — кол-во клиентов, оформивших возврат за 
рассматриваемый промежуток времени.
- period_name — weekly.
- period_id — идентификатор периода (номер недели или номер месяца).
- item_id — идентификатор категории товара.
- new_customers_revenue — доход с новых клиентов.
- returning_customers_revenue — доход с вернувшихся клиентов.
- customers_refunded — количество возвратов клиентов.   

## Реализация
Скрипты изменения и создания объектов БД, миграции данных в новую структуру в [migrations](https://github.com/SergeySenigov/data-engineer-practicum-portfolio/tree/main/03%20%D0%9C%D0%BE%D0%B4%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%86%D0%B8%D1%8F%20ETL%20%D0%B8%20%D0%B2%D0%B8%D1%82%D1%80%D0%B8%D0%BD%D1%8B.%20%D0%A0%D0%B5%D0%B0%D0%BB%D0%B8%D0%B7%D0%B0%D1%86%D0%B8%D1%8F%20%D0%B8%D0%B4%D0%B5%D0%BC%D0%BF%D0%BE%D1%82%D0%B5%D0%BD%D1%82%D0%BD%D0%BE%D1%81%D1%82%D0%B8./migrations)

Обновленный скрипт с описанием DAG "sprint3.py" в папке [dags](https://github.com/George70164/Yandex_Practicum_DE/blob/main/%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82%202/src/dags/sprint3.py)

**Этап 1 - Модифицировать процессы в пайплайне**

Так как в инкрементальных данных появилось новое поле status, то добавил его в таблицу [staging.user_order_log](https://github.com/George70164/Yandex_Practicum_DE/blob/main/%D0%9F%D1%80%D0%BE%D0%B5%D0%BA%D1%82%202/migrations/%D0%94%D0%BE%D0%B1%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5%20%D1%81%D1%82%D0%BE%D0%BB%D0%B1%D1%86%D0%B0%20status.sql). 

Сделал новое поле status обязательным и заполнил старые записи значением 'shipped'.

В файле настройки DAG создал для загрузки исторических данных второй DAG, в котором создал свой набор задач (с префиксом h_).

Изменю вызов в DAG
```sql
update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales_inc.sql", # было f_sales.sql
        parameters={"date": {business_dt}})
```

Меняю работу функций ```f_upload_data_to_staging```, ```f_upload_data_to_staging_hist``` в DAG, чтобы можно было обновлять за выборочные дни без удаления и дублирования: выполню запрос удаления ранее загруженных данных за эту дату в таблице ```user_order_log```:

Для инкрементов:
```sql
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date = '{date}'" 
engine.execute(str_del)
```

Для исторических данных
```sql
str_del = f"delete FROM {pg_schema}.{pg_table} WHERE date_time::date < '{date}'" 
engine.execute(str_del)
```

В процедуре загрузки исторических данных ```f_upload_data_to_staging_hist``` нужно подставить в URL файла вместо ```increment_id``` значение 
```report_id```, полученное ранее в ```f_get_report```

```sql
increment_id = ti.xcom_pull(key='report_id')
```

Здесь же для диагностики выведу сгруппированные данные, которые гружу из файла в ```user_order_log```
```sql
print (df.groupby(['date_time', 'status']).agg({'status': 'count', 'quantity': 'sum', 'payment_amount': 'sum'}))
```


**Этап 2 - Реализовать новую витрину**

Создал витрину по требуемой структуре.
Сделал внешний ключ по ```item_id``` к ```mart.d_item```.

```sql
drop table if exists mart.f_customer_retention ;

create table mart.f_customer_retention (
	id serial4 PRIMARY KEY,  
    new_customers_count int4 not null, 
    returning_customers_count int4 not null, 
    refunded_customer_count int4 not null, 
    period_name varchar(20) not null, 
    period_id varchar(20) not null, 
    item_id int4 not null, 
    new_customers_revenue numeric(12,2) not null, 
    returning_customers_revenue numeric(12,2) not null,
    customers_refunded numeric(12,0) not null,

    CONSTRAINT f_customer_retention_item_id_fkey FOREIGN KEY (item_id)
        REFERENCES mart.d_item (item_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION);

	CREATE INDEX IF NOT EXISTS f_cr2
    ON mart.f_customer_retention USING btree
    (item_id ASC NULLS LAST)
    TABLESPACE pg_default;

    CREATE INDEX IF NOT EXISTS f_cr3
    ON mart.f_customer_retention USING btree
    (period_id ASC NULLS LAST)
    TABLESPACE pg_default;
	
    CREATE INDEX IF NOT EXISTS f_cr4
    ON mart.f_customer_retention USING btree
    (period_name ASC NULLS LAST)
    TABLESPACE pg_default;
```

В оба DAG добавляю соответствующие задачи типа PostgresOperator: 
```sql
    h_update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention_hist.sql",
        parameters={"date": {business_dt}} )


    update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_customer_retention_inc.sql",
        parameters={"date": {business_dt}} )
```

Вношу их в дерево выполнения:
```sql
    (
            h_print_info_task 
            >> h_generate_report
            >> h_get_report
            >> h_upload_user_order
            >> [h_update_d_item_table, h_update_d_city_table, h_update_d_customer_table]
            >> h_null_task
            >> [h_update_f_sales, h_update_f_customer_retention]
    )

...

    (
            print_info_task
             >> generate_report
             >> get_report
             >> get_increment
             >> upload_user_order_inc
             >> [update_d_item_table, update_d_city_table, update_d_customer_table]
             >> null_task
             >> [update_f_sales, update_f_customer_retention]
    )
```

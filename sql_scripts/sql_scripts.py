
def sql_magic(conn):
    """
    Docstring для sql_magic
    Все скрипты в одной функции, не стал разбивать на блоки, раз и так "пашет". 
    а нам и надо чтобы сразу все скрипты отработали
   
    :param conn: даем коннект к базе
    """
    
    cursor = conn.cursor()
    cursor.execute(""" SET search_path to bank;""")
    cursor.execute(""" 
         
        --создаем таблицу отчета если ее еще нет
        CREATE table if not exists "REP_FRAUD" (
            event_dt timestamp,
            passport varchar,
            fio varchar,
            phone varchar,
            event_type varchar,
            report_dt timestamp
        );
                   
        --создаем                        
        CREATE TABLE IF NOT EXISTS "DWH_FACT_passport_blacklist"(
	        entry_dt date, 
            passport_num varchar UNIQUE, 
            create_dt date, 
            update_dt timestamp 
        );
	                
        --добавляем данные из текущего стейджа с обновлением
        --если работаем каждый день то в поле create_dt нужно указывать 
        --current_timestamp, конечно, но в ситуации когда грузим данные за несколько дней
        --думаю, нужно отражать факт время из поля "date" таблицы stg
        insert into "DWH_FACT_passport_blacklist" (
            entry_dt , 
            passport_num , 
	        create_dt , 
	        update_dt  
        )
        Select 
	        date , 
            passport ,
            current_timestamp,
            current_timestamp
        from "STG_passport_blacklist"
        ON conflict (passport_num) do update set
            entry_dt = excluded.entry_dt,
            update_dt = current_timestamp;                    

        --создаем таблицу для хранения транзакций
        CREATE TABLE IF NOT EXISTS "DWH_FACT_transactions"(
            trans_id varchar ,
            trans_date timestamp, 
            card_num varchar,
            oper_type varchar,
            amt decimal,
            oper_result varchar,
            terminal varchar,
            create_dt timestamp DEFAULT current_timestamp, 
            update_dt timestamp DEFAULT current_timestamp
        );

        --добавляем в хранилище данные из текущего стейджа 
        INSERT into "DWH_FACT_transactions" (
            trans_id ,
            trans_date , 
            card_num ,
            oper_type ,
            amt ,
            oper_result ,
            terminal 
        )
        SELECT transaction_id ,
            transaction_date ::timestamp, 
            card_num ,
            oper_type ,
            CAST(REPLACE(amount,',', '.') AS decimal(10,2)),
            oper_result ,
            terminal  
        FROM "STG_transactions" stg;
                
        --создаем хранилище для терминалов
        CREATE TABLE IF NOT EXISTS "DWH_DIM_terminals"(
            terminal_id varchar UNIQUE ,
            terminal_type varchar, 
            terminal_city varchar,
            terminal_address varchar,
            create_dt timestamp DEFAULT current_timestamp, 
            update_dt timestamp DEFAULT current_timestamp
        );

        --добавляем терминалы из текущего стейджа с обновлением
        INSERT INTO "DWH_DIM_terminals"(
            terminal_id ,
            terminal_type , 
            terminal_city ,
            terminal_address 
        )
        SELECT * FROM "STG_terminals" st 
        ON CONFLICT (terminal_id) DO UPDATE SET 
            terminal_type = excluded.terminal_type,
            terminal_city = excluded.terminal_city,
            terminal_address = excluded.terminal_address,
            update_dt = current_timestamp;

        --создаем промежуточную таблицу для детекции "отловленных" операций
        CREATE TABLE If NOT EXISTS "STG_passport_errors"(
            event_dt timestamp,
            passport varchar,
            fio varchar,
            phone varchar,
            event_type varchar, 
            report_dt timestamp
        );

        --сохраняем в "промежуток" улов (операции вне периода действия паспорта или с паспортом из стоп листа)
        INSERT INTO "STG_passport_errors"(
            event_dt,
            passport,
            fio,
            phone,
            event_type, 
            report_dt)
        SELECT
            transaction_date::timestamp AS event_dt,
            passport_num AS passport,
            concat_ws(' ', last_name, first_name, patronymic) AS fio,
            phone,
            CASE 
                WHEN cl.passport_valid_to <= t.transaction_date::timestamp::date THEN 'Expired Passport'
                WHEN bl.passport IS NOT NULL THEN 'Blacklisted Passport'
            END,
            current_timestamp::timestamp  AS report_dt
        FROM "STG_transactions" t INNER JOIN cards c ON t.card_num = c.card_num
                                  INNER JOIN accounts a ON a.account = c.account
                                  INNER JOIN clients cl ON a.client = cl.client_id
                                  LEFT JOIN "STG_passport_blacklist" bl ON cl.passport_num = bl.passport
        WHERE (passport_valid_to IS NOT NULL AND passport_valid_to <= transaction_date::timestamp::date) 
            OR bl.passport IS NOT null;

        --разбор кейса "закончился договор": создаем если нет
        CREATE TABLE IF NOT EXISTS "STG_smth_expired"(
            event_dt timestamp,
            passport varchar,
            fio varchar,
            phone varchar,
            event_type varchar, 
            report_dt timestamp
        );

        --собираем во временную, что отловили из текущего стейджа
        INSERT INTO "STG_smth_expired" (
            event_dt,
            passport,
            fio,
            phone,
            event_type, 
            report_dt)
        SELECT
            transaction_date::timestamp AS event_dt,
            passport_num AS passport,
            concat_ws(' ', last_name, first_name, patronymic) AS fio,
            phone,
            'Subscription expired' AS event_type, 
            current_timestamp::timestamp  AS report_dt
        FROM "STG_transactions" t INNER JOIN cards c ON t.card_num = c.card_num
                                  INNER JOIN accounts a ON a.account = c.account
                                  INNER JOIN clients cl ON a.client = cl.client_id
        WHERE a.valid_to <= t.transaction_date::timestamp;

        --транзакции из разных городов
        --создаем временную таблицу
        CREATE TABLE IF NOT EXISTS "STG_different_cities_fraud"(
            transaction_id varchar, 
            client_id varchar,
            card_num varchar, 
            terminal_city varchar, 
            prev_city varchar, 
            time_diff interval, 
            prev_time timestamp, 
            trans_time timestamp 
        );

        --добавляем данные соответсвующие условиям
        INSERT INTO "STG_different_cities_fraud" (
                transaction_id, 
                client_id,
                card_num, 
                terminal_city, 
                prev_city, 
                time_diff, 
                prev_time, 
                trans_time) 
        WITH total_transactions AS (
        -- Собираем данные о транзакциях по городам
            SELECT 
                t.transaction_id, 
                t.card_num, 
                cl.client_id,
                t2.terminal_city, 
                t.transaction_date::timestamp AS trans_time
            FROM "STG_transactions" t 
                INNER JOIN "STG_terminals" t2 ON t.terminal = t2.terminal_id
                INNER JOIN cards c ON t.card_num = c.card_num
                INNER JOIN accounts a ON c.account = a.account
                INNER JOIN clients cl ON a.client = cl.client_id
        )
        --селектим данные для итогового вывода, сначала отлавливал операции по одной карте, но потом 
        --решил, что правильнее смотреть вообще по клиенту, оставил так, потому как потом решил, 
        --что не все операции мошеннические, но про это ничего не было в условии, так что остановился
        --если что - пару условий в where добавить не проблема
            SELECT transaction_id, client_id, card_num, terminal_city, prev_city, time_diff, 
		        prev_time, trans_time     
            FROM (
        -- настраиваем выборку по текущим и предыдущим записям город/время
            SELECT *,
                LAG(terminal_city) OVER w as prev_city,
                LAG(trans_time) OVER w as prev_time,
                (trans_time - LAG(trans_time) OVER w) AS time_diff 
            FROM total_transactions
            WINDOW w AS (PARTITION BY client_id ORDER BY trans_time)
                ) sub
        -- проверяем, где жулики наследили
            WHERE prev_city IS NOT NULL --отсеиваем крайние, где предыдущий город не существует
                AND terminal_city <> prev_city --оно в случае если следущее условие тру
                AND time_diff < INTERVAL '1 hour'; -- если дошли и до этого условия, то точно "оно"

        --попытки подбора сумм
        --создаем временную таблицу
        CREATE table if not exists "STG_amount_bruteforce" (
            transaction_id varchar,
            trans_time timestamp,
            amount decimal,
            lag1_am varchar,
            lag2_am varchar,
            lag3_am varchar,
            "20 min interval" INTERVAL
        );

        --выбираем данные, наполняем временную таблицу
        INSERT INTO "STG_amount_bruteforce" (
            transaction_id, 
            trans_time, 
            amount , 
            lag1_am, 
            lag2_am, 
            lag3_am, 
            "20 min interval"
        )
        SELECT 
        -- селектим интересующие поля для наглядности вывода
            sub.transaction_id, 
            sub.trans_time, 
            sub.amount , 
            sub.lag1_am, 
            sub.lag2_am, 
            sub.lag3_am, 
            (sub.trans_time - sub.lag3_dt) AS "20 min interval"    
        from(
            SELECT 
            --считаем и смотрим, где жулики наследили
                transaction_id, 
                card_num, 
                CAST(REPLACE(amount,',', '.') AS decimal(10,2)) AS amount,
                oper_result, 
                transaction_date::timestamp AS trans_time,
        --используем оптимизацию в виде "именнованного окна" - вычисляем оконную функцию 1 раз для всех запросов 
                    LAG(oper_result, 1) OVER w AS lag1,
                    LAG(oper_result, 2) OVER w AS lag2,
                    LAG(oper_result, 3) OVER w AS lag3,
                    lag(CAST(REPLACE(amount,',', '.') AS decimal(10,2)), 1) OVER w AS lag1_am,
                    lag(CAST(REPLACE(amount,',', '.') AS decimal(10,2)), 2) OVER w AS lag2_am,
                    lag(CAST(REPLACE(amount,',', '.') AS decimal(10,2)), 3) OVER w AS lag3_am,
                    lag(transaction_date::timestamp, 3) OVER w AS lag3_dt
            FROM "STG_transactions"
        --а вот и окно
            WINDOW w AS (PARTITION BY card_num ORDER BY transaction_date::timestamp) ) sub
        --выборка строк с данными по условиям детекции целевых транзакций
        WHERE 	oper_result = 'SUCCESS' 
            AND lag1 = 'REJECT' 
            AND lag2 = 'REJECT' 
            AND lag3 = 'REJECT' 
            AND amount < lag1_am 
            AND lag1_am < lag2_am 
            AND lag2_am < lag3_am 
            AND trans_time - lag3_dt <= INTERVAL '20 minutes';
                   
        --грузим из временных таблиц в отчет хотя можно было и сразу
        INSERT INTO "REP_FRAUD" (
            event_dt,
            passport,
            fio,
            phone,
            event_type, 
            report_dt
        )
        SELECT * FROM "STG_passport_errors"
        UNION 
        SELECT * FROM "STG_smth_expired";

        -- грузим в отчет что нужно из временной + join-им недостающие поля 
        -- где разные города
        INSERT INTO "REP_FRAUD" (
            event_dt,
            passport,
            fio,
            phone,
            event_type, 
            report_dt)
        SELECT
            transaction_date::timestamp AS event_dt,
            passport_num AS passport,
            concat_ws(' ', last_name, first_name, patronymic) AS fio,
            phone,
            'different cities' AS event_type, 
            current_timestamp::timestamp  AS report_dt
        FROM "STG_transactions" t INNER JOIN cards c ON t.card_num = c.card_num
                            INNER JOIN accounts a ON a.account = c.account
                            INNER JOIN clients cl ON a.client = cl.client_id
                            INNER JOIN "STG_different_cities_fraud" z ON t.transaction_id::text = z.transaction_id;

        --добавляем данные в общий лог по заданной структуре по брутфорсу сумм
        INSERT INTO "REP_FRAUD" (
            event_dt,
            passport,
            fio,
            phone,
            event_type, 
            report_dt
        )
        SELECT
            transaction_date::timestamp AS event_dt,
            passport_num AS passport,
            concat_ws(' ', last_name, first_name, patronymic) AS fio,
            phone,
            'amount brute_force' AS event_type, 
            current_timestamp::timestamp  AS report_dt
        FROM "STG_transactions" t INNER JOIN cards c ON t.card_num = c.card_num
                            INNER JOIN accounts a ON a.account = c.account
                            INNER JOIN clients cl ON a.client = cl.client_id
                            INNER JOIN "STG_amount_bruteforce" z ON t.transaction_id::text = z.transaction_id;
                   
        --создаем таблицу с историей по стоп-листу паспортов
        --сделал в формате scd2, так как scd1 в том формате, как в задании
        --не учитывает возможное удаление паспорта из стоп-листа 
        CREATE TABLE IF NOT EXISTS "DWH_DIM_passport_blacklist_hist"(
	        entry_dt date, 
            passport varchar, 
	        effective_from date, 
	        effective_to timestamp, 
	        deleted_flg int
        );
	                
        --добавляем данные из текущего стейджа с обновлением
        --здесь помечаем удаленными те записи которых болеше нет в стоп-листе
        UPDATE "DWH_DIM_passport_blacklist_hist"
        SET 
	        effective_to = current_timestamp - interval '1 second',
	        deleted_flg = '1'
        WHERE deleted_flg = '0' 
	        AND passport NOT IN (SELECT passport FROM "STG_passport_blacklist");  

        --здесь вставляем новые записи                
        INSERT INTO "DWH_DIM_passport_blacklist_hist"(
	        entry_dt, 
	        passport, 
	        effective_from, 
	        effective_to, 
	        deleted_flg
        )
        SELECT 
	        spb.date,
	        spb.passport,
        -- Начинает действовать c момента, указанного в stg, полезно, когда грузим 
        --данные за несколько дней
	        spb.date,             
	        '5999-12-31 23:59:59'::timestamp, -- тех. бесконечность
	        '0'                            -- Флаг актуальности
        FROM "STG_passport_blacklist" spb
        LEFT JOIN "DWH_DIM_passport_blacklist_hist" dbl 
        ON spb.passport = dbl.passport 
	    AND dbl.deleted_flg = '0'
        WHERE dbl.passport IS NULL;        -- Только если записи еще нет

        --создаем таблицу истории по терминалам
        CREATE TABLE IF NOT EXISTS "DWH_DIM_terminals_hist"(
	        terminal_id varchar ,
	        terminal_type varchar, 
	        terminal_city varchar,
	        terminal_address varchar,
	        effective_from date, 
	        effective_to timestamp DEFAULT '5999-12-31 23:59:59'::timestamp,
	        deleted_flg int
        );

        --помечаем удаленные
        UPDATE "DWH_DIM_terminals_hist"
        SET 
	        effective_to = current_timestamp - INTERVAL '1 second',
	        deleted_flg = 1
        WHERE deleted_flg = 0
	        AND terminal_id NOT IN (SELECT terminal_id FROM "STG_terminals");

        --помечаем удаленными те записи, которые изменились
        UPDATE "DWH_DIM_terminals_hist" dwh
        SET 
            effective_to = CURRENT_TIMESTAMP - INTERVAL '1 second',
            deleted_flg = '1' -- Закрываем старую версию
        FROM "STG_terminals" st
        WHERE dwh.terminal_id = st.terminal_id
        AND dwh.deleted_flg = '0' -- Обновляем только текущую активную запись
        AND (
        --интересный синтаксис, помогает обрабатывать NULL
            st.terminal_type    IS DISTINCT FROM dwh.terminal_type OR 
            st.terminal_city    IS DISTINCT FROM dwh.terminal_city OR 
            st.terminal_address IS DISTINCT FROM dwh.terminal_address
        );

        --добавляем строки совсем новые или те, которые появляются после "удаления", в т.ч. при обновлении
        INSERT INTO "DWH_DIM_terminals_hist"(
	        terminal_id ,
	        terminal_type , 
	        terminal_city ,
	        terminal_address ,
	        effective_from,
	        deleted_flg
        )
        SELECT 
	        t1.terminal_id ,
	        t1.terminal_type , 
	        t1.terminal_city ,
	        t1.terminal_address ,
	        (SELECT st2.transaction_date  FROM "STG_transactions" st2 Limit 1)::timestamp,
	        '0'
        FROM "STG_terminals" t1
        LEFT JOIN "DWH_DIM_terminals_hist" t2 
        ON t1.terminal_id = t2.terminal_id
	        AND t2.deleted_flg = '0'
        WHERE t2.terminal_id IS NULL;
    

        --ощищаем временные таблицы перед выходом
        TRUNCATE TABLE "STG_passport_errors";
        TRUNCATE TABLE "STG_smth_expired";   
        TRUNCATE TABLE "STG_different_cities_fraud";
        TRUNCATE TABLE "STG_amount_bruteforce";            
    """)
    conn.commit()


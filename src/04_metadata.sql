
----------------------------------1--------------------------------
DROP PROCEDURE IF EXISTS proc_drop_tables_starting_with_TableName();
CREATE OR REPLACE PROCEDURE proc_drop_tables_starting_with_TableName()
AS $$
DECLARE
    table_name_to_drop TEXT; 
BEGIN
    FOR table_name_to_drop IN (SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'tablename%')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || table_name_to_drop || ' CASCADE;';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
-- Проверяем есть ли таблицы в текущей базе данных, имена которых начинаются с фразы 'TableName'.
/*
SELECT table_name
FROM information_schema.tables
WHERE table_name LIKE concat('tablename','%') AND table_schema = 'public'
*/
-- Если нет, то создаем
-- CREATE TABLE TableName_del_1 (column_1 varchar, column_2 varchar);					   
-- CREATE TABLE TableName_del_2 (column_1 varchar, column_2 varchar);
-- Вызываем процедуру
-- CALL proc_drop_tables_starting_with_TableName();
-- Еще раз проверяем запросом выше, что таких таблиц нет, то есть они удалились.

----------------------------------2--------------------------------
DROP PROCEDURE IF EXISTS proc_show_func_with_param(list_of_functions OUT TEXT, quantity OUT INTEGER);
CREATE OR REPLACE PROCEDURE proc_show_func_with_param(list_of_functions OUT TEXT, quantity OUT INTEGER)
AS $$
DECLARE table_rec record;
BEGIN
	list_of_functions := '';
	quantity := 0;
	FOR table_rec IN (SELECT
					  proname || ' ' || CONCAT_WS(', ',proargnames) AS Name
					  FROM pg_catalog.pg_proc pr
					  JOIN pg_catalog.pg_namespace ns ON ns.oid = pr.pronamespace
					  WHERE prokind = 'f'
					  AND nspname != 'pg_catalog'
					  AND nspname != 'information_schema'
					  AND proargnames IS NOT NULL)
	LOOP
		 list_of_functions := (list_of_functions || table_rec.name || '   ');
		 quantity = quantity+1;
	END LOOP;
	RETURN;
END;
$$ LANGUAGE plpgsql;

-- CALL proc_show_func_with_param('',0)

----------------------------------3--------------------------------
DROP PROCEDURE IF EXISTS proc_delete_triggers(OUT number_of_remote_triggers int);
CREATE OR REPLACE PROCEDURE proc_delete_triggers(OUT number_of_remote_triggers int)
AS $$
DECLARE
    triger_name name;
	table_trigger_name name;
    sql_request text;
BEGIN
	SELECT COUNT(DISTINCT trigger_name) INTO number_of_remote_triggers
	FROM information_schema.triggers
	WHERE trigger_schema = 'public';

   	FOR triger_name, table_trigger_name IN (SELECT DISTINCT trigger_name, event_object_table
                         FROM information_schema.triggers
                         WHERE trigger_schema = 'public')
    	LOOP
        	sql_request := 'DROP TRIGGER ' || triger_name || ' ON ' || table_trigger_name ;
        	EXECUTE sql_request;
    	END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Проверяем есть ли в текущей базее данных тригеры
/*
SELECT trigger_name
FROM information_schema.triggers
*/
-- Смотрим количество триггеров если они есть
/*
SELECT COUNT(DISTINCT trigger_name) AS amount_triggers
FROM information_schema.triggers
WHERE trigger_schema = 'public';
*/
-- Вызываем процедуру и после отработки процедуры еще раз смотрим количесвто триггеров (пункт выше)
-- CALL proc_delete_triggers(NULL);


----------------------------------4--------------------------------
DROP PROCEDURE IF EXISTS proc_search_proc_string(n IN VARCHAR, r REFCURSOR);
CREATE OR REPLACE PROCEDURE proc_search_proc_string(n VARCHAR, r REFCURSOR DEFAULT 'ref') 
AS $$
BEGIN
    OPEN r FOR
		SELECT proname AS Name, CASE prokind
								WHEN 'p' THEN 'procedure' 
								WHEN 'f' THEN 'function'
								ELSE NULL
								END AS type
		 FROM pg_catalog.pg_proc pr
		 JOIN pg_catalog.pg_namespace ns ON ns.oid = pr.pronamespace
		WHERE proname ILIKE '%' || n || '%'
		      AND nspname != 'pg_catalog'
		      AND nspname != 'information_schema';
END;
$$ LANGUAGE plpgsql;

/*
CALL proc_search_proc_string('delete');
FETCH ALL FROM "ref";
*/








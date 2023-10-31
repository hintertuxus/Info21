----------------------------------1----------------------------------
-- СОЗДАНИЕ БАЗЫ ДАННЫХ --

CREATE TABLE IF NOT EXISTS peers (
 	nickname varchar NOT NULL PRIMARY KEY,
	birthday date NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
 	title varchar NOT NULL PRIMARY KEY,
	parent_task varchar DEFAULT NULL,
	max_xp integer NOT NULL,
	FOREIGN KEY (parent_task) REFERENCES tasks (title)
);

CREATE TABLE IF NOT EXISTS checks (
 	id bigserial PRIMARY KEY,
	peer varchar NOT NULL,
	task varchar NOT NULL,
	"date" date NOT NULL,
	FOREIGN KEY (task) REFERENCES tasks (title),
	FOREIGN KEY (peer) REFERENCES peers (nickname)
);

-- создание специального типа данных для фиксации статуса проверки --
DROP TYPE IF EXISTS check_status;
CREATE TYPE check_status AS enum ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS p2p (
 	id bigserial PRIMARY KEY,
	check_id bigint NOT NULL,
	checking_peer varchar NOT NULL,
	"state" check_status NOT NULL,
	"time" time NOT NULL,
	FOREIGN KEY (check_id) REFERENCES checks (id),
	FOREIGN KEY (checking_peer) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS verter (
 	id bigserial PRIMARY KEY,
	check_id bigint NOT NULL,
	"state" check_status NOT NULL,
	"time" time NOT NULL,
	FOREIGN KEY (check_id) REFERENCES checks (id)
);

CREATE TABLE IF NOT EXISTS transferred_points (
 	id bigserial PRIMARY KEY,
	checking_peer varchar NOT NULL,
	checked_peer varchar NOT NULL,
	points_amount integer NOT NULL,
	FOREIGN KEY (checking_peer) REFERENCES peers (nickname),
	FOREIGN KEY (checked_peer) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS friends (
 	id bigserial PRIMARY KEY,
	peer_1 varchar NOT NULL,
	peer_2 varchar NOT NULL,
	FOREIGN KEY (peer_1) REFERENCES peers (nickname),
	FOREIGN KEY (peer_2) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS recommendations (
 	id bigserial PRIMARY KEY,
	peer varchar NOT NULL,
	recommended_peer varchar NOT NULL,
	FOREIGN KEY (peer) REFERENCES peers (nickname),
	FOREIGN KEY (recommended_peer) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS xp (
 	id bigserial PRIMARY KEY,
	check_id bigint NOT NULL,
	xp_amount integer NOT NULl,
	FOREIGN KEY (check_id) REFERENCES checks (id)
);

CREATE TABLE IF NOT EXISTS time_tracking (
 	id bigserial PRIMARY KEY,
	peer varchar NOT NULL,
	"date" date NOT NULL,
	"time" time NOT NULL,
	"state" integer NOT NULL,
	FOREIGN KEY (peer) REFERENCES peers (nickname),
	CONSTRAINT state_check CHECK ("state" IN (1, 2))
);

----------------------------------2----------------------------------
-- ПРОЦЕДУРЫ ИМПОРТА И ЭКСПОРТА В/ИЗ CSV --

CREATE OR REPLACE PROCEDURE proc_import_from_csv(
    IN p_table_name TEXT,
    IN p_file_path TEXT,
	IN separator char
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'COPY ' || p_table_name || ' FROM ''' || p_file_path || ''' DELIMITER ''' || separator || ''' CSV HEADER;';
END;
$$;

CREATE OR REPLACE PROCEDURE proc_export_to_csv(
    IN p_table_name TEXT,
    IN p_file_path TEXT,
	IN separator char
)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'COPY ' || p_table_name || ' TO ''' || p_file_path || ''' DELIMITER ''' || separator || ''' CSV HEADER;';
END;
$$;

----------------------------------3----------------------------------
-- ВСТАВКА ДАННЫХ В ТАБЛИЦУ ИЗ ПРЕДВАРИТЕЛЬНО ПОДГОТОВЛЕННЫХ CSV

DO $$

DECLARE vPath VARCHAR;
BEGIN
vPath := '/Users/glenniss/projects/SQL2_Info21_v1.0-1/src';

CALL proc_import_from_csv('peers', vPath || '/peers.csv', ',');
CALL proc_import_from_csv('checks', vPath || '/checks.csv', ',');
CALL proc_import_from_csv('p2p', vPath || '/p2p.csv', ',');
CALL proc_import_from_csv('tasks', vPath || '/tasks.csv', ',');
CALL proc_import_from_csv('verter', vPath || '/verter.csv', ',');
CALL proc_import_from_csv('friends', vPath || '/friends.csv', ',');
CALL proc_import_from_csv('recommendations', vPath || '/recommendations.csv', ',');
CALL proc_import_from_csv('xp', vPath || '/xp.csv', ',');
CALL proc_import_from_csv('time_tracking', vPath || '/time_tracking.csv', ',');
CALL proc_import_from_csv('transferred_points', vPath || '/transferred_points.csv', ',');

END $$;

----------------------------------4----------------------------------
-- ВЫГРУЗКА ДАННЫХ В CSV

/*

DO $$

DECLARE vPath VARCHAR;
BEGIN
vPath := '/Users/glenniss/projects/SQL2_Info21_v1.0-1/src';
CALL proc_export_to_csv('checks', vPath || '/checks.csv', ',');
CALL proc_export_to_csv('friends', vPath || '/friends.csv', ',');
CALL proc_export_to_csv('p2p', vPath || '/p2p.csv', ',');
CALL proc_export_to_csv('peers', vPath || '/peers.csv', ',');
CALL proc_export_to_csv('recommendations', vPath || '/recommendations.csv', ',');
CALL proc_export_to_csv('tasks', vPath || '/tasks.csv', ',');
CALL proc_export_to_csv('time_tracking', vPath || '/time_tracking.csv', ',');
CALL proc_export_to_csv('transferred_points', vPath || '/transferred_points.csv', ',');
CALL proc_export_to_csv('verter', vPath || '/verter.csv', ',');
CALL proc_export_to_csv('xp', vPath || '/xp.csv', ',');

END $$;

*/

-- 5 --
-- отдельно 
-- для первоначального рассчета перераспределения пойнтов
/*
INSERT INTO transferred_points (checking_peer, checked_peer, points_amount)
	SELECT checking_peer, peer, count(*)
	  FROM p2p
	  JOIN checks c on c.id = p2p.check_id
	 WHERE state != 'Start'
	 GROUP BY 1,2;

*/



----------------------------------1----------------------------------
-- ПРОЦЕДУРА ДОБАВЛЕНИЯ P2P ПРОВЕРКА --

CREATE OR REPLACE PROCEDURE proc_add_p2p_check(
								IN checked  VARCHAR,
								IN checker VARCHAR,
								IN task_name VARCHAR,
								IN status CHECK_STATUS,
								IN "time" TIME) AS $$
	BEGIN
	   IF (status = 'Start') THEN
			INSERT INTO checks VALUES (
				(SELECT count(*) + 1 FROM checks),
				checked,
				task_name,
				now()
				);
			INSERT INTO p2p VALUES (
				(SELECT count(*) + 1 FROM p2p),
				(SELECT MAX(checks.id) FROM checks WHERE peer = checked AND task = task_name),
				checker,
				status,
				time
				);
	  ELSE
		WITH lch AS (
			SELECT c.id AS check_id, max(p2p.time)
			FROM p2p
			JOIN checks AS c
				ON  c.Task = task_name
				AND c.Peer = checked
			WHERE p2p.state = 'Start'
			GROUP BY c.id
			ORDER BY 1 DESC
			LIMIT 1
		)
		INSERT INTO p2p VALUES (
			(SELECT count(*) + 1 FROM p2p),
			(SELECT check_id FROM lch),
			checker,
			status,
			time
			);
		END IF;
	END;
$$ LANGUAGE plpgsql;

-- TESTING -- 
/*
-- starting p2p
CALL proc_add_p2p_check('Dan', 'Bob', 'C5_s21_decimal', 'Start', '10:11:00');

-- failing p2p
CALL proc_add_p2p_check('Dan', 'Bob', 'C5_s21_decimal', 'Failure', '10:11:00');


DELETE FROM p2p WHERE id = (SELECT MAX(id) FROM p2p);
DELETE FROM checks WHERE id = (SELECT MAX(id) FROM checks);

select * FROM p2p;
select * FROM checks;
select * FROM transferred_points;
*/


----------------------------------2----------------------------------
--ПРОЦЕДУРА ДОБАВЛЕНИЯ АВТОМАТИЧЕСКОЙ ПРОВЕРКИ--

CREATE OR REPLACE PROCEDURE proc_add_verter_check(
					IN checked_peer varchar,
					IN task_name text,
					IN verter_status check_status,
					IN verter_time time) AS $$
	BEGIN
		IF EXISTS (SELECT check_id, MAX(p2p.time)
				FROM p2p
				JOIN checks AS c
					ON  p2p.check_id = c.id
					AND c.peer = checked_peer
				WHERE p2p.state = 'Success' AND c.task = task_name
				GROUP BY check_id
				ORDER BY 1 DESC, 2 DESC
				LIMIT 1)
		THEN
			IF (verter_status = 'Start')
			THEN
				INSERT INTO verter
				VALUES ((SELECT COUNT(*) FROM verter) + 1,
						(SELECT check_id, MAX(p2p.time)
				FROM p2p
				JOIN checks AS c
					ON  p2p.check_id = c.id
					AND c.peer = checked_peer
				WHERE p2p.state = 'Success' AND c.task = task_name
				GROUP BY check_id
				ORDER BY 1 DESC
				LIMIT 1),
						verter_status, verter_time);
      		ELSE
				INSERT INTO verter
				VALUES ((SELECT COUNT(*) FROM verter) + 1,
						(SELECT check_id
				FROM p2p
				JOIN checks AS c
					ON  p2p.check_id = c.id
					AND c.peer = checked_peer
				WHERE p2p.state = 'Success' AND c.task = task_name
				GROUP BY check_id
				ORDER BY 1 DESC
				LIMIT 1), verter_status, verter_time);                
            END IF;
        ELSE
			RAISE EXCEPTION 'P2P-check is not finished or has status Failure';			
        END IF;
    END;
$$ LANGUAGE plpgsql;


--FOR TESTING--

--select * from verter
--CALL proc_add_verter_check('Bob', 'C4_s21_math', 'Success', '21:45:00');
--DELETE FROM verter WHERE id = (SELECT MAX(id) FROM verter);


----------------------------------3----------------------------------
-- ТРИГГЕР --

CREATE OR REPLACE FUNCTION fnc_trg_update_transferred_poins()
	RETURNS TRIGGER AS
$trg_update_transferred_points$
BEGIN
	IF (new.state = 'Start') THEN
		WITH tmp AS (
               SELECT checks.peer AS peer 
			   FROM p2p
               JOIN checks 
			   ON p2p.check_id = checks.id
			   AND new.check_id = checks.id
           )
		UPDATE transferred_points
		   SET points_amount = points_amount + 1
		  FROM tmp
		 WHERE transferred_points.checked_peer = tmp.peer
		 	AND transferred_points.checking_peer = new.checking_peer;
	END IF;
	IF ((SELECT COUNT(*)
		  FROM transferred_points
		  WHERE checked_peer = (SELECT peer FROM checks WHERE id = new.check_id)
		  AND checking_peer = new.checking_peer) = 0) THEN
			  INSERT INTO transferred_points (id, checking_peer, checked_peer, points_amount)
			  VALUES ((SELECT count(*) + 1 FROM transferred_points), new.checking_peer, 
					  (SELECT peer FROM checks WHERE id = new.check_id),
					  1);
	END IF;
	RETURN NULL;
	END;
$trg_update_transferred_points$ LANGUAGE plpgsql;

-- создаем триггер--
 CREATE TRIGGER trg_update_transferred_points
  AFTER INSERT ON p2p
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_update_transferred_poins();

-- TESTS -- 
/*
CALL proc_add_p2p_check('Dan', 'Bob', 'C5_s21_decimal', 'Start', '10:11:00');

select * FROM p2p;
select * FROM checks;

select * FROM transferred_points;

*/


----------------------------------4----------------------------------
--ТРИГГЕР--

CREATE OR REPLACE FUNCTION fnc_trg_validate_xp_insert() RETURNS TRIGGER
AS $$
    BEGIN
        IF ((SELECT max_xp FROM checks
            JOIN tasks
			ON checks.task = tasks.title
            WHERE NEW.check_id = checks.id) < NEW.xp_amount OR
            (SELECT state FROM p2p
             WHERE NEW.check_id = p2p.check_id AND p2p.state IN ('Success', 'Failure')) = 'Failure' OR
            (SELECT state FROM verter
             WHERE NEW.check_id = verter.check_id AND verter.state = 'Failure') = 'Failure') 
		THEN
			 	RAISE EXCEPTION 'Некорректная запись XP';
				
				
		END IF;
		
	RETURN (NEW.id, NEW.check_id, NEW.xp_amount);
    END;
$$ LANGUAGE plpgsql;

-- Создаем триггер
CREATE TRIGGER trg_xp_insert_validation
BEFORE INSERT ON xp
FOR EACH ROW
EXECUTE FUNCTION fnc_trg_validate_xp_insert();


--FOR TESTING--
/*
select * from xp

INSERT INTO xp (id, check_id, xp_amount)
VALUES (14, 14, 670);

DELETE FROM xp WHERE id = (SELECT MAX(id) FROM xp)
*/

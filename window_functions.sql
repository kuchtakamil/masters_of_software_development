CREATE TABLE ministry_name (
	id SERIAL PRIMARY KEY,
	name VARCHAR(50)
);

INSERT INTO ministry_name (name) VALUES
  ('Ministry of War'),
  ('Ministry of Science'),
  ('Ministry of Lies'),
  ('Ministry of Defence'),
  ('Ministry of Adventure'),
  ('Ministry of Cooking');

CREATE TABLE expense (
  id SERIAL PRIMARY KEY,
  subject VARCHAR(50),
  transaction_date date,
  amount integer,
  ministry_name_id integer REFERENCES ministry_name(id)
);

INSERT INTO expense (subject, transaction_date, amount, ministry_name_id) VALUES
  ('Galaxy Destroyer', '5023-12-16', 500000, 1),
  ('Laser Beam', '5023-12-16', 3000, 1),
  ('Shield Barrier', '5023-12-18', 5200, 1),
  ('Cloaking System', '5023-12-18', 5200, 1),
  ('Healing Drones', '5023-12-18', 500, 1),
  ('Fighters Fleet', '5023-12-18', 22500, 1),
  ('Quantum Portal', '5023-12-18', 94000, 2),
  ('Time Inverter', '5023-12-19', 89000, 2),
  ('Destructor Cannon', '5023-12-19', 19000, 1),
  ('Subspace Drive', '5023-12-19', 85000, 1),
  ('Communication System', '5023-12-20', 6000, 1);


-- over na całości
SELECT 
	*,
	MAX(amount) OVER() AS max_amount,
	MIN(amount) OVER() AS min_amount
FROM expense;

-- można zwykłym SQLem ale potrzebne jest podzapytanie
SELECT 
    e.*,
    (SELECT MAX(amount) FROM expense) AS max_amount,
    (SELECT MIN(amount) FROM expense) AS min_amount
FROM expense e;

-- partition by - tworzymy okienko
SELECT 
	*,
	MAX(amount) OVER(partition by transaction_date) AS maximum_amount,
	MIN(amount) OVER() AS min_amount
FROM expense e ;

-- max i avg
SELECT 
	subject, transaction_date, amount,
	MAX(amount) OVER(partition by transaction_date) AS maximum_score,
	AVG(amount) OVER(partition by transaction_date) AS minimum_score
FROM expense e ;

-- Bieżąca Suma Kumulatywna
select distinct transaction_date, sum(amount) over(order by transaction_date) as current_sum
from expense order by transaction_date;

select distinct transaction_date, sum(amount) over(partition by transaction_date) as current_sum
from expense order by transaction_date;

-- wersja z JOINem
select e.transaction_date, mn.name AS ministry_name,
  SUM(e.amount) OVER(PARTITION BY e.transaction_date) AS per_day_sum
FROM
  expense e
JOIN 
  ministry_name mn ON e.ministry_name_id = mn.id
ORDER BY
  e.transaction_date;

-- ruchoma średnia, Moving average
SELECT transaction_date, amount, 
       AVG(amount) OVER(ORDER BY transaction_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_average
FROM expense;


-- N PRECEDING
-- N FOLLOWING
-- CURRENT ROW
-- UNBOUNDED PRECEDING/FOLLOWING
SELECT transaction_date, amount, 
       AVG(amount) OVER(ORDER BY transaction_date ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_average
FROM expense;

-- ROW_NUMBER 
select transaction_date, row_number() over(partition by transaction_date) as current_sum
from expense order by transaction_date;
-- sprobuj tez bez partition by

-- RANK i DENSE_RANK
SELECT
	*,
	RANK() OVER(PARTITION BY ministry_name_id ORDER BY amount DESC)	
FROM expanse;

SELECT
	*,
	DENSE_RANK() OVER(PARTITION BY ministry_name_id ORDER BY amount DESC)	
FROM expense;

-- LAG, LEAD
SELECT
	*,
	LAG(amount) OVER(PARTITION BY ministry_name_id ORDER BY amount DESC),	
  LEAD(amount) OVER(PARTITION BY ministry_name_id ORDER BY amount DESC)	
FROM expense;
-- LAG i LEAD przyjmują też drugi parametr np LAG(amount, 2)

-- NTILE
-- Kwantyle są to takie wartości cechy, które dzielą badaną zbiorowość na określone, równe części pod względem liczebności.
SELECT
  mn.name AS ministry_name,
  e.amount,
  NTILE(3) OVER(ORDER BY e.amount) AS expense_group
FROM
  expense e
JOIN
  ministry_name mn ON e.ministry_name_id = mn.id
ORDER BY
  e.amount;
-- dodaj partition by mn.name
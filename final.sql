SELECT ('ФИО: ПЕРЕСЫПКИН СЕРГЕЙ ВЯЧЕСЛАВОВИЧ');

cd Desktop/Netology/DZ/final/ && sudo cp dvdrental.tar /tmp/data

psql --host $APP_POSTGRES_HOST -U postgres -c "CREATE DATABASE dvdrental;" && pg_restore --host $APP_POSTGRES_HOST -c -U postgres -d dvdrental -v "/data/dvdrental.tar" -W

psql --host $APP_POSTGRES_HOST -U postgres -d dvdrental


-- Запрос 1. Состоит из нескольких запросов (1.1 -1.5). Рекомендательная система для клиента: по взятым в прокат дискам определяется
-- вероятный любимый актер (актеры): берется актер (актеры), снимавшиеся в макс. количестве фильмах, взятых в прокат клиентом. 
-- Клиенту рекомендуются фильмы с этим же актером (актерами), которые он еще не брал в прокат.

-- Запрос 1.1. Любимый актер клиента (фильмы с каким актером (актерами) чаще всего брал в прокат клиент).
-- Создается таблица с id клиента (customer_id), id любимого актера (actor_id) и количеством фильмов с любимым 
-- актером, взятых в прокат клиентом.

DROP TABLE IF EXISTS customer_favorite_actor;

CREATE TABLE customer_favorite_actor (
customer_id integer,
actor_id smallint,
actor_sum smallint);

INSERT INTO customer_favorite_actor 
(
    WITH tmp AS
    (
        SELECT customer.customer_id AS customer_id, film_actor.actor_id AS actor_id,
        -- количество фильмов с данным актером, которые брал в прокат клиент
        COUNT(film_actor.actor_id) AS actor_sum, 
        RANK() OVER(PARTITION BY customer.customer_id ORDER BY COUNT(film_actor.actor_id) DESC) AS rank_of_actor
        FROM customer
        JOIN rental ON customer.customer_id = rental.customer_id
        JOIN inventory ON rental.inventory_id = inventory.inventory_id
        JOIN film_actor ON inventory.film_id = film_actor.film_id
        JOIN actor ON film_actor.actor_id = actor.actor_id
        GROUP BY customer.customer_id, film_actor.actor_id
        ORDER BY customer.customer_id, actor_sum DESC, film_actor.actor_id
    )
    SELECT customer_id, actor_id, actor_sum
    FROM tmp
    WHERE rank_of_actor = 1
    ORDER BY customer_id
);

SELECT * FROM customer_favorite_actor
LIMIT 10;


-- Запрос 1.2. Все фильмы в которых снимался (снимались) любимый актер (актеры), каждого клиента.
-- Создается таблица с id клиента (customer_id), массивом с id любимых актеров (fav_actors) и массивом с 
-- id фильмов, в которых снимался любимый актер (актеры).
DROP TABLE IF EXISTS favorite_actors_films;

CREATE TABLE favorite_actors_films (
customer_id integer,
fav_actors int[],
film_ids int[]);

INSERT INTO favorite_actors_films 
(
    SELECT customer_id, array_agg(DISTINCT film_id), array_agg(DISTINCT customer_favorite_actor.actor_id)
    FROM customer_favorite_actor
    JOIN film_actor ON film_actor.actor_id = customer_favorite_actor.actor_id
    GROUP BY customer_id
    ORDER BY customer_id
);

SELECT * FROM favorite_actors_films
LIMIT 10;


-- Запрос 1.3. Фильмы, с любимым (любимыми) актером (актерами), которые клиент уже брал в прокат.
-- Создается таблица с id клиента (customer_id) и массивом с id фильмов, в которых снимался любимый актер (актеры)
-- и которые клиент уже брал в прокат.

DROP TABLE IF EXISTS watched_films;

CREATE TABLE watched_films (
customer_id integer,
watched_films_ids int[]);

INSERT INTO watched_films 
(
    SELECT customer.customer_id, array_agg(DISTINCT film_actor.film_id) AS watched_films_ids
    FROM customer
    JOIN rental ON customer.customer_id = rental.customer_id
    JOIN favorite_actors_films ON customer.customer_id = favorite_actors_films.customer_id
    JOIN inventory ON inventory.inventory_id = rental.inventory_id
    JOIN film_actor ON inventory.film_id = film_actor.film_id
    WHERE film_actor.actor_id = ANY(favorite_actors_films.fav_actors)
    GROUP BY customer.customer_id
    ORDER BY customer.customer_id
);

SELECT * FROM watched_films
LIMIT 10;


-- Функция считающая разницу двух массивов
CREATE OR REPLACE FUNCTION diff_arr(int[], int[]) 
RETURNS int[] language sql AS
$$ 
    SELECT ARRAY(
    SELECT UNNEST($1)
    EXCEPT
    SELECT UNNEST($2)); 
$$;

-- Запрос 1.4. Фильмы с любимым актером (актерами), которые клиент еще не смотрел.
-- Создается таблица с id клиента (customer_id) и массивом с id фильмов, в которых снимался любимый актер (актеры)
-- и которые клиент еще не брал в прокат.

DROP TABLE IF EXISTS recomended_films;

CREATE TABLE recomended_films (
customer_id integer,
recomended_films_ids int[]);

INSERT INTO recomended_films 
(
    SELECT favorite_actors_films.customer_id, diff_arr(favorite_actors_films.film_ids, watched_films_ids) AS recomended_films_with_fav_actors
    FROM favorite_actors_films
    JOIN watched_films ON favorite_actors_films.customer_id = watched_films.customer_id
    ORDER BY favorite_actors_films.customer_id
);

SELECT * FROM recomended_films
LIMIT 10;


-- Запрос 1.5. Названия и описания рекомендованных фильмов, включая актерский состав
SELECT customer_id, film.title, film.description, array_agg(actor.first_name || ' ' || actor.last_name) AS starring
FROM recomended_films
JOIN film ON film.film_id = ANY(recomended_films_ids)
JOIN film_actor ON film_actor.film_id = ANY(recomended_films_ids)
JOIN actor ON actor.actor_id = film_actor.actor_id
GROUP BY customer_id, film.title, film.description
ORDER BY customer_id
LIMIT 10;


-- Запрос 2. В какой день недели у магазина проката дисков больше всего доход.
-- Выводится день недели (day) с максимальной выручкой и суммарная выручка в этот день.
WITH tmp AS(
    SELECT DISTINCT to_char(rental_date, 'day') AS day, SUM(amount) AS total_payment
    FROM rental
    JOIN payment ON rental.rental_id = payment.rental_id
    GROUP BY to_char(rental_date, 'day')
    ) 
SELECT day, total_payment
FROM tmp
WHERE total_payment = (SELECT MAX(total_payment) FROM tmp);


-- Запрос 3. Популярность фильмов у клиентов в зависимости от возрастного рейтенга.
-- Выводится возрастной рейтинг (rating) и суммарное количество взятых в аренду дисков  с данным рейтингом (payment_num).
WITH tmp AS (
    SELECT film_id, COUNT(payment_id) AS payment_num
    FROM inventory
    JOIN rental ON rental.inventory_id = inventory.inventory_id
    JOIN payment ON rental.rental_id = payment.rental_id
    GROUP BY film_id
    ORDER BY payment_num DESC
)
SELECT DISTINCT rating, SUM(payment_num)
FROM film
JOIN tmp ON film.film_id = tmp.film_id
GROUP BY rating
ORDER BY SUM(payment_num) DESC;


-- Запрос 4. Среднее время в часах на которое берется в аредну диск.
SELECT ROUND(AVG(total_rent_duration)) AS average_rent_duration
FROM(
    SELECT DATE_PART('day', return_date - rental_date)*24 + DATE_PART('hour', return_date - rental_date) AS total_rent_duration
    FROM rental
) AS tmp;


-- Запрос 5. У каждого диска есть стандартный срок аренды (таблица films - rental_duration).
-- Высчитывается суммарное время превышения стандартных сроков аренды (total_rent_delay) для каждого клиента (customer_id) в днях.
WITH tmp AS(
    SELECT customer.customer_id, 
    CASE
        WHEN 
           rental_duration*24 <= (DATE_PART('day', return_date - rental_date)*24 + DATE_PART('hour', return_date - rental_date))
        THEN
            (rental_duration*24 - DATE_PART('day', return_date - rental_date)*24 - DATE_PART('hour', return_date - rental_date))::int
        ELSE
            0
    END AS rental_duration_diff
    FROM customer
    JOIN rental ON customer.customer_id = rental.customer_id
    JOIN inventory ON rental.inventory_id = inventory.inventory_id
    JOIN film ON inventory.film_id = film.film_id
    ORDER BY customer.customer_id
    )
SELECT customer_id, ABS(SUM(rental_duration_diff)/24) AS total_rent_delay
FROM tmp
GROUP BY customer_id
ORDER BY SUM(rental_duration_diff)
LIMIT 10;

-- Запрос 6. Зависимость полученной прибыли от стоимости аренды диска.
-- выводится стоимость аренды диска (rental_rate) и суммарная средняя прибыль, которую приносит 1
-- диск с данной стоимостью аренды (income_per_disc). 
SELECT rental_rate, SUM(amount)/COUNT(inventory.inventory_id) AS income_per_disc
FROM payment 
JOIN rental ON payment.rental_id = rental.rental_id
JOIN inventory ON inventory.inventory_id = rental.inventory_id
JOIN film ON inventory.film_id = film.film_id
GROUP BY rental_rate
ORDER BY SUM(amount)/COUNT(inventory.inventory_id) DESC;





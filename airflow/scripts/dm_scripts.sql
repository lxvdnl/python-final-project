---------DM_ORDERS------

with current_driver as (
    select 
        order_id,
        driver_id
    from delivery_assignments
    where unassigned_at is null
)
select o.order_id,
o.user_id,
o.store_id,
cd.driver_id, -- ID если заказ еще не завершен, null если завершен
sum(  (oi.item_quantity * oi.item_price * (1- oi.item_discount::NUMERIC/100))*(1- o.order_discount::NUMERIC/100)  ) as cash_flows , --обороты
sum(case 
when status = 'canceled' then 0
else (oi.item_quantity * oi.item_price * (1- oi.item_discount::NUMERIC/100))*(1- o.order_discount::NUMERIC/100)
end ) as revenue, --выручка
o.status,
order_cancellation_reason,
case -- отмена из-за ошибок на стороне сервиса (для причин отмены "Ошибка приложения" и "Проблемы с оплатой"
when o.status = 'canceled' 
and o.order_cancellation_reason in ('Ошибка приложения', 'Проблемы с оплатой')
then 1 else 0 
end as is_cancel_error,
case -- отмена после доставки
when o.delivered_at is not null 
and o.canceled_at is not null
and o.canceled_at > o.delivered_at
then 1 else 0 
end as is_cancel_after_delivery,
SPLIT_PART(address_text, ',', 1) as city,
date_part('year', o.created_at) as year,
date_part('month', o.created_at) as month,
date_part('day', o.created_at) as day
from orders o 
left join order_items oi on o.order_id = oi.order_id
left join stores s on o.store_id = s.store_id 
left join current_driver cd on o.order_id = cd.order_id
group by 
o.order_id, 
o.user_id,
cd.driver_id,
o.store_id,
status, 
order_cancellation_reason,
SPLIT_PART(address_text, ',', 1),
o.created_at;


------------DM_ITEMS----------
select 
    o.store_id,
    i.item_category,
    i.item_id,
    i.item_title,
    sum(
        oi.item_quantity * oi.item_price * (1 - oi.item_discount::numeric/100)
    ) as item_cash_flows,

    sum(oi.item_quantity) as ordered_qty,

    sum(oi.item_canceled_quantity) as canceled_qty,

    count(distinct o.order_id) as orders_cnt,

    count(distinct case 
        when oi.item_canceled_quantity > 0 then o.order_id 
    end) as orders_with_canceled_items,
	date_part('year', o.created_at) as year,
    date_part('month', o.created_at) as month,
    date_part('day', o.created_at) as day,

    split_part(o.address_text, ',', 1) as city

from orders o
left join order_items oi on o.order_id = oi.order_id
left join items i on oi.item_id = i.item_id
group by 
    year,
    month,
    day,
    city,
    o.store_id,
    i.item_category,
    i.item_id,
    i.item_title;

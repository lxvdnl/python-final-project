-- Пользователи
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    user_phone VARCHAR(20) NOT NULL
);

-- Магазины
CREATE TABLE IF NOT EXISTS stores (
    store_id INT PRIMARY KEY,
    store_address VARCHAR(255) NOT NULL
);

-- Курьеры
CREATE TABLE IF NOT EXISTS drivers (
    driver_id INT PRIMARY KEY,
    driver_phone VARCHAR(20) NOT NULL
);

-- Товары
CREATE TABLE IF NOT EXISTS items (
    item_id INT PRIMARY KEY,
    item_title VARCHAR(100) NOT NULL,
    item_category VARCHAR(50) NOT NULL
);

-- Заказы
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    store_id INT NOT NULL,
    address_text VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type VARCHAR(20) NOT NULL,
    order_discount INT DEFAULT 0,
    order_cancellation_reason VARCHAR(100),
    delivery_cost INT NOT NULL,
    status VARCHAR(20) GENERATED ALWAYS AS (
        CASE
            WHEN canceled_at IS NOT NULL THEN 'canceled'
            WHEN delivered_at IS NOT NULL THEN 'delivered'
            WHEN delivery_started_at IS NOT NULL THEN 'in_delivery'
            WHEN paid_at IS NOT NULL THEN 'paid'
            ELSE 'created'
        END
    ) STORED,

    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_store_id ON orders(store_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- Состав заказа (с группировкой)
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    item_id INT NOT NULL,
    item_quantity INT NOT NULL CHECK (item_quantity > 0),
    item_price INT NOT NULL CHECK (item_price > 0),
    item_discount INT DEFAULT 0 CHECK (item_discount >= 0 AND item_discount <= 100),
    item_canceled_quantity INT DEFAULT 0 CHECK (item_canceled_quantity >= 0),
    replaced_by_item_id INT,

    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (item_id) REFERENCES items(item_id),
    FOREIGN KEY (replaced_by_item_id) REFERENCES items(item_id)
);

-- Создаем уникальный индекс отдельно (вместо UNIQUE в определении таблицы)
CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_order_item_group
ON order_items (order_id, item_id, item_price, item_discount, COALESCE(replaced_by_item_id, -1));

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_item_id ON order_items(item_id);

-- История назначений курьеров
CREATE TABLE IF NOT EXISTS delivery_assignments (
    assignment_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    driver_id INT NOT NULL,
    assigned_at TIMESTAMP NOT NULL,
    unassigned_at TIMESTAMP,

    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),

    CHECK (unassigned_at IS NULL OR unassigned_at >= assigned_at)
);

CREATE INDEX IF NOT EXISTS idx_delivery_assignments_order_id ON delivery_assignments(order_id);
CREATE INDEX IF NOT EXISTS idx_delivery_assignments_driver_id ON delivery_assignments(driver_id);
CREATE INDEX IF NOT EXISTS idx_delivery_assignments_current ON delivery_assignments(order_id) WHERE unassigned_at IS NULL;





create table if not exists orders_datamart (
    order_id int primary key,
    user_id int not null,
    store_id int not null,

    driver_id int,

    -- деньги
    cash_flows numeric(14,2), -- оборот
    revenue numeric(14,2),    -- выручка

    -- статусы
    status varchar(20),
    order_cancellation_reason varchar(100),

    -- флаги
    is_cancel_error int,
    is_cancel_after_delivery int,

    -- разрезы
    city varchar(100),
    year int,
    month int,
    day int
);




CREATE INDEX idx_orders_dm_date 
ON orders_datamart (year, month, day);


CREATE INDEX idx_orders_dm_store 
ON orders_datamart (store_id);


CREATE INDEX idx_orders_dm_city 
ON orders_datamart (city);


CREATE INDEX idx_orders_dm_status 
ON orders_datamart (status);


CREATE INDEX idx_orders_dm_user 
ON orders_datamart (user_id);


create table if not exists items_datamart (
    store_id int not null,
    item_category varchar(100),
    item_id int not null,
    item_title varchar(255),
    item_cash_flows numeric(14,2),
    ordered_qty int,
    canceled_qty int,
    orders_cnt int,
    orders_with_canceled_items int,
    year int,
    month int,
    day int,
    city varchar(100),
    primary key (store_id, item_id, year, month, day, city)
);
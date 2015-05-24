create table Customer (
  customerId varchar(20) NOT NULL,
  name VARCHAR(32) NOT NULL,
  address VARCHAR(200) NULL,

  CONSTRAINT customer_pk PRIMARY KEY (customerId)
);

INSERT INTO Customer (customerId, name, address) values ('customer1', 'Stefano', 'Stefano address' );
INSERT INTO Customer (customerId, name ) values ('customer2', 'Luca' );
INSERT INTO Customer (customerId, name, address) values ('customer3', 'Tiago', 'Another address' );
INSERT INTO Customer (customerId, name, address) values ('customer4', 'Antonios', 'home' );
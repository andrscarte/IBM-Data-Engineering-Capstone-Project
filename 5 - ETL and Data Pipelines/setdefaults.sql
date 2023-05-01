alter table sales_data
alter column timestamp set data type timestamp;

alter table sales_data
alter column timestamp set not null;

alter table sales_data
alter column timestamp set default current_timestamp;

alter table sales_data
alter column price set not null;

alter table sales_data
alter column price set default 0;

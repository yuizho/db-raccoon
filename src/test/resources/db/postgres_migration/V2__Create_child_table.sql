create table child(
    id int not null,
    name varchar(100) not null,
    parent_id int not null,
    PRIMARY KEY(id),
    CONSTRAINT fk_parent
		FOREIGN KEY(parent_id)
		REFERENCES parent (id)
		ON DELETE NO ACTION
		ON UPDATE NO ACTION
);

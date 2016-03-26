DROP TABLE IF EXISTS Persons;

-- CREATE TABLE  IF NOT EXISTS Persons
-- (
-- PersonID int
-- );

-- INSERT INTO Persons VALUES(1);

-- SELECT * FROM Persons;

CREATE TABLE Persons
(
PersonID int,
LastName varchar(255),
FirstName varchar(255),
Address varchar(255),
City varchar(255)
); 

INSERT INTO Persons
VALUES ('1', 'san', 'sid', 'centre', 'pitt');

INSERT INTO Persons
VALUES ('2', 'san2', 'sid2', 'centr2e', 'pefeitt');

INSERT INTO Persons
VALUES ('3', 'san3', 'sid3', 'centr3e', 'pefeitrwt');

SELECT * FROM Persons;

SELECT * FROM Persons WHERE personid=1;
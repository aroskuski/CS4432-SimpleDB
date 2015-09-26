/*Our queries can also be located in the SampleQueries.java file. Running this
with the server creates the tables and runs our queries*/

CREATE TABLE Gear (GName: Varchar(20) Primary Key, BrandId: INTEGER, MAbility: Varchar(20));

CREATE TABLE Brand (Bid: INTEGER Primary Key, BName: Varchar(20), FavoredSub: Varchar(20));

INSERT INTO Gear (GName, BrandId, MAbility) VALUES ('Gas Mask', 2, 'Tenacity'),
					         ('Moto Boots', 1, 'Quick Respawn'),
					         ('Cherry Kicks', 1, 'Stealth Jump'),
					         ('Octo Tee', 3, 'Haunt'),
					         ('Tinted Shades', 4, 'Last-Ditch Effort'),
					         ('Basic Tee', 5, 'Quick Respawn');

INSERT INTO Gear (Bid, BName, FavoredSub) VALUES (1, 'Rockenberg', 'Run Speed Up'),
					         (2, 'Forge', 'Special Duration Up'),
					         (3, 'Cuttlegear', 'NULL'),
					         (4, 'Zekko', 'Special Saver'),
					         (5, 'Squidforce', 'Damage Up'),
					         (6, 'Inkline', 'Defense Up'),
					         (7, 'Tentatek', 'Ink Recovery Up');

SELECT GName, BName, FavoredSub
FROM Gear G, Brand B
WHERE G.BrandId = B.Bid

/*The expected output of this query is as follows:
Gas Mask Forge Special Duration Up
Moto Boots Rockenberg Run Speed Up
Cherry Kicks Rockenberg Run Speed Up
Octo Tee Cuttlegear Haunt
Tinted Shades Zekko Special Saver
Basic Tee Squidforce Damage Up*/
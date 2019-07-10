GRANT SELECT ON ALL TABLES IN SCHEMA public TO yupana;

CREATE TABLE organisations(
    id SERIAL PRIMARY KEY,
    type TEXT,
    org_id TEXT
);

CREATE TABLE kkms(
    id SERIAL PRIMARY KEY,
    name TEXT,
    device_id TEXT,
    org_id INTEGER REFERENCES organisations(id)
);

GRANT SELECT ON ALL TABLES IN SCHEMA public TO yupana;

INSERT INTO organisations(id, type, org_id) VALUES (1, 'Супермаркет', '54321');
INSERT INTO organisations(id, type, org_id) VALUES (2, 'Аптека', '1234567');
INSERT INTO organisations(id, type, org_id) VALUES (3, 'Автомастерская', '223322');

INSERT INTO kkms(id, name, device_id, org_id) VALUES (1, 'Касса №1', '1', 1);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (2, 'Касса №2', '12',  2);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (3, 'Касса №3', '42', 1);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (4, 'Касса №4', '4', 1);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (5, 'Касса №5', '7', 2);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (6, 'Касса №6', '5', 1);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (7, 'Касса №7', '6', 3);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (8, 'Касса №8', '8', 3);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (9, 'Касса №9', '9', 2);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (10, 'Касса №10','11', 3);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (11, 'Касса №11', '10', 1);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (12, 'Касса №12', '12', 2);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (13, 'Касса №13', '14', 1);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (14, 'Касса №14', '15', 1);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (15, 'Касса №15', '13', 3);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (16, 'Касса №16', '16', 2);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (17, 'Касса №17', '17', 1);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (18, 'Касса №18', '18', 2);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (19, 'Касса №19', '19', 3);
INSERT INTO kkms(id, name, device_id, org_id) VALUES (20, 'Касса №20', '20', 1);

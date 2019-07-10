CREATE TABLE public.t1 (
    id integer NOT NULL,
    k text NOT NULL,
    t2_id integer,
    ff1 text
);

CREATE TABLE public.t2 (
    id integer NOT NULL,
    ff2 text
);

CREATE TABLE public.test_catalog (
    kkm_id character varying(50) NOT NULL,
    f1 text,
    f2 text
);

INSERT INTO public.t1 (id, k, t2_id, ff1) VALUES (1, '123432345654', 1, 'hhh');
INSERT INTO public.t1 (id, k, t2_id, ff1) VALUES (2, '123432345655', 1, 'hhh2');
INSERT INTO public.t1 (id, k, t2_id, ff1) VALUES (3, '123432345656', 2, 'hhh2');
INSERT INTO public.t1 (id, k, t2_id, ff1) VALUES (4, '123432345657', 3, 'hhh3');

INSERT INTO public.t2 (id, ff2) VALUES (1, 'ggg');
INSERT INTO public.t2 (id, ff2) VALUES (2, 'ggg2');
INSERT INTO public.t2 (id, ff2) VALUES (3, 'ggg3');

INSERT INTO public.test_catalog (kkm_id, f1, f2) VALUES ('123432345654', 'qwe', 'asd');
INSERT INTO public.test_catalog (kkm_id, f1, f2) VALUES ('123432345655', 'wer', 'sdf');
INSERT INTO public.test_catalog (kkm_id, f1, f2) VALUES ('123432345656', 'ert', 'dfg');
INSERT INTO public.test_catalog (kkm_id, f1, f2) VALUES ('123432345657', 'rty', 'fgh');

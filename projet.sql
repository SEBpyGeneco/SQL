Departements(dep_num VARCHAR(3) NOT NULL,
            reg_num SMALLINT,
            cheflieu VARCHAR(5) NOT NULL,
            tncc SMALLINT,
            ncc TEXT NOT NULL,
            nccenr TEXT NOT NULL,
            libelle TEXT NOT NULL,
            PRIMARY KEY (dep_num),
            FOREIGN KEY (reg_num) REFERENCES Regions(reg_num))

Regions(reg_num SMALLINT,
        cheflieu VARCHAR(5) NOT NULL,
        tncc SMALLINT,
        ncc TEXT NOT NULL,
        nccenr TEXT NOT NULL,
        libelle TEXT NOT NULL,
        PRIMARY KEY (reg_num))


INSERT INTO stage.landing_scada_data (
        unit_id, 
        location_id, 
        var_id, 
        measure, 
        moddate,   
        modtime
    ) VALUES (
        :unit_idn, 
        :unit_location_idn, 
        :var_idn, 
        :measure, 
        :moddate, 
        :modtime
    )
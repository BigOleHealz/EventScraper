
SELECT_SOURCES = """
    SELECT
        _id as source_id,
        source,
        source_url
    FROM
        events.sources;
"""

SELECT_EVENT_TYPE_SOURCE_MAPPINGS = """
    SELECT
        _id as source_event_type_mapping_id,
        source_id,
        target_event_type_uuid,
        source_event_type_id,
        source_event_type_string
    FROM
        events.event_type_source_mappings;
"""

SELECT_EVENT_TYPES = """
    SELECT
        UUID as event_type_uuid,
        EventType as event_type
    FROM
        events.event_types;
"""

SELECT_REGIONS = """
    SELECT
        _id as region_id,
        city_code,
        state_code,
        country_code
    FROM
        events.regions;
"""
SELECT_INGESTION_ATTEMPTS_FOR_DATES_AFTER_TODAY = """
    SELECT
        source_id,
        region_id,
        date,
        source_event_type_mapping_id
    FROM
        events.ingestions
    WHERE date >= '{start_date}'
        AND
    ingestion_status = 'SUCCESS';
"""

INSERT_INGESTION_ATTEMPT = """
    INSERT INTO events.ingestions (
        UUID,
        source_id,
        region_id,
        date,
        source_event_type_mapping_id,
        ingestion_status,
        ingestions_start_ts,
        success_count,
        error_count,
        virtual_count
    ) VALUES('{UUID}', {source_id}, {region_id}, '{date}', {source_event_type_mapping_id}, 'INITIATED', CURRENT_TIMESTAMP, NULL, NULL, NULL);
"""

UPDATE_INGESTION_ATTEMPT_STATUS = """
    UPDATE events.ingestions
    SET ingestion_status='{status}'
    WHERE UUID='{UUID}';
"""

CHECK_IF_EVENT_EXISTS = """

    SELECT
        UUID
    FROM
        events.events_successful
    WHERE
        SourceEventID = {SourceEventID};
"""
    
CLOSE_INGESTION_ATTEMPT = """
    UPDATE events.ingestions
    SET ingestion_status='{status}',
        success_count={success_count},
        error_count={error_count},
        virtual_count={virtual_count}
    WHERE UUID='{UUID}';
"""

INSERT_RAW_EVENT = """
    INSERT INTO events.events_raw(
        UUID,
        Source,
        SourceID,
        EventURL,
        ingestion_status,
        ingestion_uuid,
        region_id,
        event_start_date,
        s3_link,
        error_message
    ) VALUES( 
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
"""


UPDATE_RAW_EVENT_INGESTION_STATUS = """
    UPDATE events.events_raw
    SET
        ingestion_status='{status}',
        error_message='{error_message}'
    WHERE UUID='{UUID}';
"""

INSERT_EVENT_SUCCESSFULLY_INGESTED = """
    INSERT INTO events.events_successful (
        UUID,
        Address,
        EventType,
        EventTypeUUID,
        StartTimestamp,
        EndTimestamp,
        ImageURL,
        Host,
        Lon,
        Lat,
        Summary,
        PublicEventFlag,
        FreeEventFlag,
        Price,
        EventDescription,
        EventName,
        SourceEventID,
        EventPageURL
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
"""

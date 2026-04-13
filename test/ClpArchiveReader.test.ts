import {
    ClpArchiveReader,
    type FieldValue,
} from "clp-ffi-js/sfa";
import {
    afterEach,
    describe,
    expect,
    it,
} from "vitest";

import {loadTestData} from "./utils.js";


const CLP_JSON_TEST_LOG_FILES_EXPECTED_FILE_COUNT = 9;
const CLP_JSON_TEST_LOG_FILES_EXPECTED_EVENT_COUNT = 132n;
const COCKROACHDB_EXPECTED_EVENT_COUNT = 200000n;
const COCKROACHDB_TIMESTAMP_CHECK_COUNT = 1000;
const MAX_LOG_EVENT_INDEX_CHECKS = 1000;
const POSTGRESQL_EXPECTED_EVENT_COUNT = 1000000n;
const TRITON_EXPECTED_EVENT_COUNT = 1699n;

/**
 *
 * @param reader
 * @param expectedCount
 */
const assertLogEventIndices = (reader: ClpArchiveReader, expectedCount: bigint): void => {
    const decodedEvents = reader.decodeAll();
    expect(decodedEvents.length).toBe(Number(expectedCount));

    const numChecks = Math.min(decodedEvents.length, MAX_LOG_EVENT_INDEX_CHECKS);
    for (let i = 0; i < numChecks; i += 1) {
        const event = decodedEvents[i];
        expect(event).toBeDefined();
        expect(event?.logEventIdx).toBe(BigInt(i));
    }
};

/**
 *
 * @param value
 */
const parseTimestampFieldToMs = (value: FieldValue): bigint | null => {
    if ("number" === typeof value) {
        return BigInt(Math.trunc(value));
    }
    if ("string" === typeof value) {
        if (value.includes(".")) {
            const asNumber = Number.parseFloat(value);
            if (Number.isFinite(asNumber)) {
                return BigInt(Math.trunc(asNumber * 1000));
            }

            return null;
        }
        if ((/^-?\d+$/).test(value)) {
            return BigInt(value);
        }
    }

    return null;
};

describe("ClpArchiveReader", () => {
    let reader: ClpArchiveReader | null = null;
    let reader2: ClpArchiveReader | null = null;
    let readerWoTs: ClpArchiveReader | null = null;

    const createReaderFromArchive = async (archiveFilename: string): Promise<ClpArchiveReader> => {
        const data = await loadTestData(archiveFilename);
        return await ClpArchiveReader.create(data);
    };

    afterEach(() => {
        if (null !== reader) {
            reader.close();
            reader = null;
        }
        if (null !== readerWoTs) {
            readerWoTs.close();
            readerWoTs = null;
        }
    });

    it("should read postgresql sfa archive from buffer", async () => {
        reader = await createReaderFromArchive("postgresql.clp");

        expect(reader.getEventCount()).toBe(POSTGRESQL_EXPECTED_EVENT_COUNT);
        assertLogEventIndices(reader, POSTGRESQL_EXPECTED_EVENT_COUNT);
    });

    it("should read cockroachdb sfa archive from buffer", async () => {
        reader = await createReaderFromArchive("cockroachdb.clp");

        const dataWoTs = await loadTestData("cockroachdb_wo_ts.clp");
        readerWoTs = await ClpArchiveReader.create(dataWoTs);

        expect(reader.getEventCount()).toBe(COCKROACHDB_EXPECTED_EVENT_COUNT);
        expect(readerWoTs.getEventCount()).toBe(COCKROACHDB_EXPECTED_EVENT_COUNT);
        assertLogEventIndices(reader, COCKROACHDB_EXPECTED_EVENT_COUNT);
        assertLogEventIndices(readerWoTs, COCKROACHDB_EXPECTED_EVENT_COUNT);

        const decodedEvents = reader.decodeAll();
        const decodedEventsWoTs = readerWoTs.decodeAll();
        expect(decodedEvents.length).toBe(Number(COCKROACHDB_EXPECTED_EVENT_COUNT));
        expect(decodedEventsWoTs.length).toBe(decodedEvents.length);

        for (let i = 0; i < decodedEvents.length; i += 1) {
            expect(decodedEventsWoTs[i]?.message).toBe(decodedEvents[i]?.message);
        }

        for (let i = 0; i < Math.min(COCKROACHDB_TIMESTAMP_CHECK_COUNT, decodedEvents.length); i += 1) {
            const event = decodedEvents[i];
            const eventWoTs = decodedEventsWoTs[i];
            expect(event).toBeDefined();
            expect(eventWoTs).toBeDefined();
            expect(eventWoTs?.logEventIdx).toBe(event?.logEventIdx);
            expect(eventWoTs?.timestamp).toBe(0n);
            const kvPairs = event?.getKvPairs();
            expect(kvPairs).not.toBeNull();
            const timestampField = kvPairs?.["timestamp"];
            expect(timestampField).toBeDefined();
            const parsedTimestamp = parseTimestampFieldToMs(timestampField as FieldValue);
            expect(parsedTimestamp).not.toBeNull();
            expect(parsedTimestamp).toBe(event?.timestamp);
        }
    });

    it("should read clp_json_test_log_files sfa archive from buffer", async () => {
        const data = await loadTestData("clp_json_test_log_files.clp");
        reader = await ClpArchiveReader.create(data);

        const fileNames = reader.getFileNames();
        expect(fileNames.length).toBe(CLP_JSON_TEST_LOG_FILES_EXPECTED_FILE_COUNT);

        const fileInfos = reader.getFileInfos();
        expect(fileInfos.length).toBe(CLP_JSON_TEST_LOG_FILES_EXPECTED_FILE_COUNT);

        expect(reader.getEventCount()).toBe(CLP_JSON_TEST_LOG_FILES_EXPECTED_EVENT_COUNT);
        assertLogEventIndices(reader, CLP_JSON_TEST_LOG_FILES_EXPECTED_EVENT_COUNT);
    });

    it("should read clp_json_test_log_files sfa archive from buffer", async () => {
        reader = await createReaderFromArchive("clp_json_test_log_files.clp");

        const fileNames = reader.getFileNames();
        expect(fileNames.length).toBe(CLP_JSON_TEST_LOG_FILES_EXPECTED_FILE_COUNT);

        const fileInfos = reader.getFileInfos();
        expect(fileInfos.length).toBe(CLP_JSON_TEST_LOG_FILES_EXPECTED_FILE_COUNT);
        expect(fileInfos.map((fileInfo) => fileInfo.fileName)).toEqual(fileNames);

        expect(reader.getEventCount()).toBe(CLP_JSON_TEST_LOG_FILES_EXPECTED_EVENT_COUNT);

        const initialEventCount = 0n;
        const sum = fileInfos.reduce(
            (eventCountSum, fileInfo) => eventCountSum + fileInfo.logEventCount,
            initialEventCount
        );

        expect(sum).toBe(CLP_JSON_TEST_LOG_FILES_EXPECTED_EVENT_COUNT);
    });

    it("should decode triton sfa archive text consistently", async () => {
        reader = await createReaderFromArchive("triton.clp");

        expect(reader.getEventCount()).toBe(TRITON_EXPECTED_EVENT_COUNT);
        const logEventMessages = reader.decodeAll().map((event) => `${event.message}`);

        reader2 = await createReaderFromArchive("triton.clp");
        const readableStream = await reader2.getReadableStream();
        const streamReader = readableStream.getReader();
        const decoder = new TextDecoder();
        let bufferedText = "";
        let decodedMessageIdx = 0;

        while (true) {
            const {done, value} = await streamReader.read();
            if (done) {
                break;
            }
            bufferedText += decoder.decode(value, {stream: true});

            const decodedLines = bufferedText.match(/[^\n]*\n/g) ?? [];
            let consumedTextLength = 0;
            for (const decodedLine of decodedLines) {
                const decodedMessage = logEventMessages[decodedMessageIdx];
                expect(decodedLine).toBe(decodedMessage);

                consumedTextLength += decodedLine.length;
                decodedMessageIdx += 1;
            }
            bufferedText = bufferedText.slice(consumedTextLength);
        }
        bufferedText += decoder.decode();

        expect(bufferedText).toBe("");
        expect(decodedMessageIdx).toBe(logEventMessages.length);
    });

    it("should throw when calling getEventCount after close", async () => {
        const closedReader = await createReaderFromArchive("postgresql.clp");
        closedReader.close();

        expect(() => closedReader.getEventCount()).toThrow();
    });

    it("should not throw when calling close multiple times", async () => {
        const closedReader = await createReaderFromArchive("postgresql.clp");
        closedReader.close();

        expect(() => {
            closedReader.close();
        }).not.toThrow();
    });
});

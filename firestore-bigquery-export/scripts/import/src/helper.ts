/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { BigQuery } from "@google-cloud/bigquery";

import { CliConfig } from "./types";

import {
  ChangeType,
  FirestoreDocumentChangeEvent,
} from "@firebaseextensions/firestore-bigquery-change-tracker";
import * as firebase from "firebase-admin";
import * as fs from "fs";
import * as util from "util";

import { resolveWildcardIds } from "./config";

const FIRESTORE_DEFAULT_DATABASE = "(default)";

/**
 * The import only writes rows: it never creates or modifies the dataset,
 * table schema, partitioning, clustering or views, which the extension
 * manages. Fails if the raw changelog table isn't there to import into.
 */
export const assertChangelogTableExists = async (config: CliConfig) => {
  const bigquery = new BigQuery({ projectId: config.bigQueryProjectId });

  const table = bigquery
    .dataset(config.datasetId, { location: config.datasetLocation })
    .table(config.rawChangeLogName);

  const [tableExists] = await table.exists();
  if (!tableExists) {
    throw new Error(
      `Table ${config.bigQueryProjectId}.${config.datasetId}.${config.rawChangeLogName} does not exist. ` +
        "Install the extension (or let it create the table) before importing."
    );
  }
};

export function getRowsFromDocs(
  docs: firebase.firestore.QueryDocumentSnapshot<firebase.firestore.DocumentData>[],
  config: CliConfig
): FirestoreDocumentChangeEvent[] {
  let rows: FirestoreDocumentChangeEvent[] = [];

  const templateSegments = config.sourceCollectionPath.split("/");

  if (config.queryCollectionGroup && templateSegments.length > 1) {
    for (const doc of docs) {
      let pathParams = {};
      const path = doc.ref.path;

      const pathSegments = path.split("/");
      const isSameLength = pathSegments.length === templateSegments.length + 1;

      if (isSameLength) {
        let isMatch = true;
        for (let i = 0; i < templateSegments.length; i++) {
          if (
            templateSegments[i].startsWith("{") &&
            templateSegments[i].endsWith("}")
          ) {
            const key = templateSegments[i].substring(
              1,
              templateSegments[i].length - 1
            );
            const value = pathSegments[i];
            pathParams = {
              ...pathParams,
              [key]: value,
            };
          } else if (templateSegments[i] !== pathSegments[i]) {
            isMatch = false;
            break;
          }
        }
        if (isMatch) {
          rows.push({
            // The document's last write time, so changes the extension
            // recorded after it still win in the latest view.
            timestamp: doc.updateTime.toDate().toISOString(),
            operation: ChangeType.IMPORT,
            documentName: `projects/${config.projectId}/databases/${FIRESTORE_DEFAULT_DATABASE}/documents/${path}`,
            documentId: doc.id,
            // TODO: fix this type
            // @ts-expect-error
            pathParams,
            eventId: "",
            data: doc.data(),
          });
        }
      }
    }
  } else {
    // TODO: fix this type
    // @ts-expect-error
    rows = docs.map((snapshot) => {
      return {
        timestamp: snapshot.updateTime.toDate().toISOString(),
        operation: ChangeType.IMPORT,
        documentName: `projects/${config.projectId}/databases/${FIRESTORE_DEFAULT_DATABASE}/documents/${snapshot.ref.path}`,
        documentId: snapshot.id,
        pathParams: resolveWildcardIds(
          config.sourceCollectionPath,
          snapshot.ref.path
        ),
        eventId: "",
        data: snapshot.data(),
      };
    });
  }
  return rows;
}

const appendFile = util.promisify(fs.appendFile);

/**
 * Initializes or resets the failed batch output file.
 * If a file already exists at the specified path, it will be deleted and recreated.
 * This ensures we start with a clean file for each import operation.
 *
 * @param failedBatchOutputPath - Path where failed batch information should be stored
 *                               If undefined, the function returns early (no file operations)
 * @throws Error if file deletion fails
 */
export async function initializeFailedBatchOutput(
  failedBatchOutputPath?: string
) {
  if (!failedBatchOutputPath) return;

  // Delete existing file if present
  // This prevents append operations from mixing old and new failed files
  if (fs.existsSync(failedBatchOutputPath)) {
    try {
      await fs.promises.unlink(failedBatchOutputPath);
      console.log(`${failedBatchOutputPath} was deleted successfully.`);
    } catch (err) {
      throw new Error(`Error deleting ${failedBatchOutputPath}: ${err}`);
    }
  }

  // Create a new empty file
  // This ensures the file exists before any append operations
  await fs.promises.writeFile(failedBatchOutputPath, "", "utf8");
}

/**
 * Records a failed batch of documents to the output file.
 * Each document's path is written on a new line for easy parsing.
 * Failed batches are appended to the file, preserving previous failures.
 *
 * @param failedBatchOutputPath - Path to the file where failed batches should be recorded
 *                               If undefined, the function returns early (no recording)
 * @param docs - Array of Firestore documents that failed to import
 *              Their paths will be extracted and recorded
 */
export async function recordFailedBatch(
  failedBatchOutputPath: string | undefined,
  docs: firebase.firestore.QueryDocumentSnapshot<firebase.firestore.DocumentData>[]
) {
  if (!failedBatchOutputPath) return;

  try {
    // Convert document array to newline-separated paths
    // Add final newline to maintain one-path-per-line format
    await appendFile(
      failedBatchOutputPath,
      docs.map((d) => d.ref.path).join("\n") + "\n"
    );
  } catch (error) {
    console.error(`Error writing to failed batch file: ${error}`);
  }
}

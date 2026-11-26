import { nanoid } from "@/lib/nanoid";
import { redis } from "@/lib/redis";
import { s } from "@upstash/redis";

// Reference chunks are stored as JSON keys and indexed with Upstash Redis
// Search. `content` is a HYBRID field: Redis Search embeds it server-side,
// so it can be searched by meaning without a separate embedding provider.
const CHUNK_INDEX = "chunks";
const CHUNK_KEY_PREFIX = "chunk:";

const chunkSchema = s.object({
	content: s.hybrid(),
	userId: s.keyword(),
	referenceId: s.keyword(),
});

const chunkIndex = redis.search.index({
	name: CHUNK_INDEX,
	schema: chunkSchema,
});

// Score cutoffs for `$ann` results (0..1, higher is closer). Measured on
// Redis Search's embeddings: relevant chunks score about 0.73-0.82 and
// unrelated ones about 0.48-0.58, so the old Upstash Vector cutoffs
// (0.8 / 0.875) dropped most relevant chunks.
// Below this, a chunk is not used as context.
export const MIN_CHUNK_SCORE = 0.65;
// Below this, the selected references are not a good match and the query
// falls back to all of the user's references.
export const STRONG_CHUNK_SCORE = 0.7;

let indexReady: Promise<unknown> | null = null;

// Creates the index on first use. `existsOk` makes this a no-op when the
// index already exists with the same schema.
function ensureChunkIndex() {
	if (!indexReady) {
		indexReady = redis.search
			.createIndex({
				name: CHUNK_INDEX,
				dataType: "json",
				prefix: CHUNK_KEY_PREFIX,
				schema: chunkSchema,
				existsOk: true,
			})
			.catch((error) => {
				indexReady = null;
				throw error;
			});
	}
	return indexReady;
}

export async function upsertChunks(
	chunks: string[],
	metadata: { userId: string; referenceId: string },
) {
	if (chunks.length === 0) return;

	await ensureChunkIndex();

	const pipeline = redis.pipeline();
	for (const content of chunks) {
		pipeline.json.set(`${CHUNK_KEY_PREFIX}${nanoid()}`, "$", {
			content,
			userId: metadata.userId,
			referenceId: metadata.referenceId,
		});
	}
	await pipeline.exec();
}

export async function queryChunks({
	query,
	userId,
	referenceIds,
	limit = 5,
}: {
	query: string;
	userId: string;
	referenceIds?: string[];
	limit?: number;
}): Promise<{ score: number; data: string }[]> {
	if (!query.trim()) return [];

	await ensureChunkIndex();

	const results = await chunkIndex.query({
		filter: {
			$ann: {
				field: "content",
				query,
				filter:
					referenceIds && referenceIds.length > 0
						? {
								userId: { $eq: userId },
								referenceId: { $in: referenceIds },
							}
						: { userId: { $eq: userId } },
			},
		},
		limit,
	});

	return (results ?? []).map((result) => ({
		score: result.score,
		data: result.data.content,
	}));
}

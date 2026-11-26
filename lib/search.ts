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

// The `@nestjs/workflows/core` entry: the engine's storage-agnostic pieces, for building packages on them (a queue,
// say) so they behave and read like workflows. Applications import `@nestjs/workflows`; nothing here is needed to
// write or run workflows. Nothing here imports a driver.

// Time: durations, and the clock every timestamp comes from
export { ManualClock, parseDuration, systemClock, type Duration } from './time/index.js';
export type { Clock } from './interfaces/index.js';

// Retries: the family's retry settings, resolved, and whether and when to retry after a failure
export { backoffDelay, nextRetry, resolveRetry, type RetryDecision } from './retries/index.js';
export type { BackoffSettings, ResolvedBackoff, ResolvedRetry, RetrySettings } from './interfaces/index.js';

// Payloads: codecs, the AES-GCM one, and the family's envelope that stores what they encode
export { AesGcmPayloadCodec, isEncodedPayload, PayloadCodecs, type AesGcmPayloadCodecOptions, type PayloadCodecsOptions } from './codecs/index.js';
export type { PayloadCodec, PayloadContext, SerializedError } from './interfaces/index.js';

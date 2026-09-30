/**
 * Vitest global setup of the MySQL projects: generates the Prisma client that `fromPrisma()`'s MySQL tests run on
 * (`@prisma/adapter-mariadb`) into tests/fixtures/prisma-mysql/generated (gitignored).
 */
import { generatePrismaClient } from './generate-prisma-client.js';

export default function generateMysqlPrismaClient() {
  generatePrismaClient('tests/fixtures/prisma-mysql/prisma.config.ts');
}

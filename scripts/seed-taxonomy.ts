#!/usr/bin/env bun

/**
 * Seed the taxonomy database from data/taxonomy.json
 *
 * Loads the SKOS taxonomy structure and populates the database
 * with concepts and hierarchical relationships.
 */

import { Effect } from "effect";
import os from "node:os";
import { join } from "node:path";
import { readFileSync } from "node:fs";
import {
  TaxonomyService,
  TaxonomyServiceImpl,
  type TaxonomyJSON,
} from "../src/services/TaxonomyService.js";

const DB_PATH = join(os.homedir(), "Documents", ".pdf-library", "library.db");

const TAXONOMY_FILE = join(import.meta.dir, "..", "data", "taxonomy.json");

async function seedTaxonomy() {
  try {
    // Load taxonomy JSON
    console.log(`📖 Loading taxonomy from: ${TAXONOMY_FILE}`);
    const taxonomyData = readFileSync(TAXONOMY_FILE, "utf-8");
    const taxonomy = JSON.parse(taxonomyData) as TaxonomyJSON;

    console.log(`   Found ${taxonomy.concepts.length} concepts`);
    console.log(
      `   Found ${taxonomy.hierarchy?.length || 0} hierarchy relationships\n`
    );

    // Create service layer
    const layer = TaxonomyServiceImpl.make({ url: `file:${DB_PATH}` });

    // Seed the database
    const program = Effect.gen(function* () {
      const service = yield* TaxonomyService;

      console.log("🌱 Seeding taxonomy...");
      yield* service.seedFromJSON(taxonomy);

      console.log("✅ Taxonomy seeded successfully!");

      // Display root concepts
      console.log("\n📊 Root Concepts:");
      const rootIds = [
        "programming",
        "education",
        "business",
        "design",
        "meta",
      ];

      for (const id of rootIds) {
        const concept = yield* service.getConcept(id);
        if (concept) {
          const narrower = yield* service.getNarrower(id);
          console.log(
            `   • ${concept.prefLabel} (${narrower.length} children)`
          );
        }
      }
    });

    // Run the program
    await Effect.runPromise(Effect.provide(program, layer));

    console.log("\n✨ Done!\n");
  } catch (error) {
    console.error("❌ Error seeding taxonomy:", error);
    process.exit(1);
  }
}

seedTaxonomy();

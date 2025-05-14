import {
  createTranslationsTable,
  populateFieldTranslations,
} from "../lib/db/fieldTranslations";

async function main() {
  try {
    console.log("Initializing field translations table...");
    await createTranslationsTable();
    console.log("Table created successfully.");

    console.log("\nPopulating field translations...");
    await populateFieldTranslations();
    console.log("Field translations populated successfully.");
  } catch (error) {
    console.error(
      "Error:",
      error instanceof Error ? error.message : String(error)
    );
    process.exit(1);
  }
}

main();

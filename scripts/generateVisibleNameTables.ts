import { generateVisibleNameTables } from "../lib/db/tables/generateVisibleNameTables";

async function main() {
  try {
    console.log("Starting generation of visible name tables...");
    await generateVisibleNameTables();
    console.log("Successfully generated visible name tables!");
  } catch (error) {
    console.error("Error generating visible name tables:", error);
    process.exit(1);
  }
}

main();

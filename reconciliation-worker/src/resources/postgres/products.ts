/**
 * @file Defines parameterized SQL for worker product observations and conditional repairs.
 * Adapters bind values and execute these statements; this module performs no database I/O.
 */

/** Read the managed fields, validation timestamp, and local note for the product bound at $1. */
export const selectProductSql = `SELECT product_id, name, description, category, source_revision,
        applied_at, internal_note FROM public.products WHERE product_id=$1`;

/** Insert an absent copy with a null local note; a concurrent existing identifier remains untouched. */
export const insertProductSql = `INSERT INTO public.products
          (product_id,name,description,category,source_revision,internal_note)
          VALUES ($1,$2,$3,$4,$5,NULL) ON CONFLICT (product_id) DO NOTHING RETURNING source_revision`;

/** Advance only the exact observed revision at $6 to the newer revision at $5, preserving local data. */
export const advanceProductSql = `UPDATE public.products SET name=$2,description=$3,category=$4,
        source_revision=$5,applied_at=clock_timestamp()
        WHERE product_id=$1 AND source_revision=$6 AND source_revision<$5 RETURNING source_revision`;

import json
import logging
import re
import hashlib
import shutil
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, Iterator, Optional

import kuzu

logger = logging.getLogger(__name__)


class GraphIngester:
    def __init__(self, db_path: str | Path = "kuzu_db"):
        """Initialize connection to Kuzu database and create schema if needed."""
        self.db_path = str(db_path)
        # Ensure parent directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Connecting to KuzuDB at {self.db_path}")
        self.db = kuzu.Database(self.db_path)
        self.conn = kuzu.Connection(self.db)
        if not self._schema_is_compatible():
            self._backup_incompatible_database()
            self.db = kuzu.Database(self.db_path)
            self.conn = kuzu.Connection(self.db)
        self._init_schema()

    def _schema_is_compatible(self) -> bool:
        """Check whether an existing graph DB has the current ID-based schema."""
        try:
            result = self.conn.execute("CALL table_info('Invoice') RETURN *")
        except Exception:
            return True

        while result.has_next():
            row = result.get_next()
            if row[1] == "id" and row[4] is True:
                return True
        return False

    def _backup_incompatible_database(self) -> None:
        """Move an old graph index aside before creating the current schema."""
        logger.warning(
            "Existing Kuzu graph schema is incompatible. Moving old graph DB aside "
            "and rebuilding a fresh index."
        )
        self.conn.close()
        self.db.close()

        db_path = Path(self.db_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = db_path.with_name(f"{db_path.name}.legacy_{timestamp}")
        shutil.move(str(db_path), str(backup_path))
        logger.warning(f"Old graph DB moved to {backup_path}")

    def _init_schema(self):
        """Create graph node tables and relationship tables if they don't exist."""
        nodes = [
            """
            CREATE NODE TABLE IF NOT EXISTS Document(
                id STRING,
                result_path STRING,
                output_folder STRING,
                source_file STRING,
                PRIMARY KEY (id)
            )
            """,
            """
            CREATE NODE TABLE IF NOT EXISTS Vendor(
                id STRING,
                name STRING,
                normalized_name STRING,
                tax_id STRING,
                email STRING,
                phone STRING,
                street STRING,
                city STRING,
                state STRING,
                postal_code STRING,
                country STRING,
                PRIMARY KEY (id)
            )
            """,
            """
            CREATE NODE TABLE IF NOT EXISTS Customer(
                id STRING,
                name STRING,
                normalized_name STRING,
                tax_id STRING,
                email STRING,
                phone STRING,
                street STRING,
                city STRING,
                state STRING,
                postal_code STRING,
                country STRING,
                PRIMARY KEY (id)
            )
            """,
            """
            CREATE NODE TABLE IF NOT EXISTS Invoice(
                id STRING,
                invoice_number STRING,
                invoice_date STRING,
                due_date STRING,
                currency STRING,
                subtotal DOUBLE,
                tax_total DOUBLE,
                total DOUBLE,
                amount_paid DOUBLE,
                amount_due DOUBLE,
                purchase_order STRING,
                notes STRING,
                page INT64,
                PRIMARY KEY (id)
            )
            """,
            """
            CREATE NODE TABLE IF NOT EXISTS LineItem(
                id STRING,
                description STRING,
                quantity DOUBLE,
                unit_price DOUBLE,
                amount DOUBLE,
                sku STRING,
                unit STRING,
                PRIMARY KEY (id)
            )
            """,
            """
            CREATE NODE TABLE IF NOT EXISTS Tax(
                id STRING,
                tax_type STRING,
                rate DOUBLE,
                amount DOUBLE,
                PRIMARY KEY (id)
            )
            """,
        ]

        rels = [
            """CREATE REL TABLE IF NOT EXISTS EXTRACTED(FROM Document TO Invoice)""",
            """CREATE REL TABLE IF NOT EXISTS ISSUED(FROM Vendor TO Invoice)""",
            """CREATE REL TABLE IF NOT EXISTS BILLED_TO(FROM Invoice TO Customer)""",
            """CREATE REL TABLE IF NOT EXISTS CONTAINS(FROM Invoice TO LineItem)""",
            """CREATE REL TABLE IF NOT EXISTS HAS_TAX(FROM Invoice TO Tax)""",
        ]

        for q in nodes + rels:
            try:
                self.conn.execute(q)
            except RuntimeError as e:
                if "already exists" not in str(e).lower():
                    logger.warning(f"Schema creation issue for query '{q}': {e}")

    def _execute_cypher(self, query: str, parameters: Optional[Dict[str, Any]] = None):
        """Execute a cypher query with parameters."""
        if parameters is None:
            parameters = {}
        try:
            return self.conn.execute(query, parameters)
        except Exception as e:
            logger.error(f"Error executing query: {query}\nParams: {parameters}\nError: {e}")
            raise

    def ingest_json(
        self,
        file_path: Path,
        source_file: Optional[Path] = None,
        output_folder: Optional[Path] = None,
    ) -> bool:
        """Parse a result.json file and ingest into the graph."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return False

        records = list(self._iter_invoice_records(data))
        if not records:
            logger.warning(f"No invoice data found in {file_path}, skipping.")
            return False

        document_id = self._document_id(file_path, source_file)
        output_folder = output_folder or file_path.parent
        self._upsert_document(document_id, file_path, source_file, output_folder)

        ingested = 0
        for record_index, (page, invoice) in enumerate(records):
            if self._ingest_invoice(document_id, file_path, invoice, page, record_index):
                ingested += 1

        logger.info(f"Ingested {ingested} invoice record(s) from {file_path}.")
        return ingested > 0

    def _iter_invoice_records(self, data: Any) -> Iterator[tuple[Optional[int], dict[str, Any]]]:
        """Yield normalized invoice records from flat, batch, or PDF page-shaped JSON."""
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    payload = item.get("data", item)
                    yield from self._iter_invoice_records(payload)
            return

        if not isinstance(data, dict):
            return

        pages = data.get("pages")
        if isinstance(pages, list):
            for page in pages:
                if not isinstance(page, dict):
                    continue
                page_num = page.get("page")
                payload = page.get("data")
                if isinstance(payload, dict):
                    yield int(page_num) if isinstance(page_num, int) else None, payload
            return

        if "data" in data and isinstance(data["data"], dict):
            yield from self._iter_invoice_records(data["data"])
            return

        yield None, data

    def _ingest_invoice(
        self,
        document_id: str,
        file_path: Path,
        data: dict[str, Any],
        page: Optional[int],
        record_index: int,
    ) -> bool:
        vendor = data.get("vendor") if isinstance(data.get("vendor"), dict) else {}
        customer = data.get("customer") if isinstance(data.get("customer"), dict) else {}
        invoice_id = self._invoice_id(data, vendor, file_path, page, record_index)
        payment_terms = data.get("payment_terms") if isinstance(data.get("payment_terms"), dict) else {}

        self.conn.execute("BEGIN TRANSACTION")
        try:
            self._execute_cypher(
                """
                MERGE (i:Invoice {id: $id})
                SET
                    i.invoice_number = coalesce($invoice_number, i.invoice_number),
                    i.invoice_date = coalesce($invoice_date, i.invoice_date),
                    i.due_date = coalesce($due_date, i.due_date),
                    i.currency = coalesce($currency, i.currency),
                    i.subtotal = coalesce($subtotal, i.subtotal),
                    i.tax_total = coalesce($tax_total, i.tax_total),
                    i.total = coalesce($total, i.total),
                    i.amount_paid = coalesce($amount_paid, i.amount_paid),
                    i.amount_due = coalesce($amount_due, i.amount_due),
                    i.purchase_order = coalesce($purchase_order, i.purchase_order),
                    i.notes = coalesce($notes, i.notes),
                    i.page = coalesce($page, i.page)
                """,
                {
                    "id": invoice_id,
                    "invoice_number": self._nullable_string(data.get("invoice_number")),
                    "invoice_date": self._nullable_string(data.get("invoice_date")),
                    "due_date": self._nullable_string(payment_terms.get("due_date")),
                    "currency": self._nullable_string(data.get("currency")),
                    "subtotal": self._nullable_number(data.get("subtotal")),
                    "tax_total": self._nullable_number(data.get("tax_total")),
                    "total": self._nullable_number(data.get("total")),
                    "amount_paid": self._nullable_number(data.get("amount_paid")),
                    "amount_due": self._nullable_number(data.get("amount_due")),
                    "purchase_order": self._nullable_string(data.get("purchase_order")),
                    "notes": self._nullable_string(data.get("notes")),
                    "page": page,
                },
            )

            self._execute_cypher(
                """
                MATCH (d:Document {id: $document_id}), (i:Invoice {id: $invoice_id})
                MERGE (d)-[:EXTRACTED]->(i)
                """,
                {"document_id": document_id, "invoice_id": invoice_id},
            )

            if vendor and vendor.get("name"):
                vendor_id = self._party_id("vendor", vendor)
                self._upsert_party("Vendor", vendor_id, vendor)
                self._execute_cypher(
                    """
                    MATCH (v:Vendor {id: $vendor_id}), (i:Invoice {id: $invoice_id})
                    MERGE (v)-[:ISSUED]->(i)
                    """,
                    {"vendor_id": vendor_id, "invoice_id": invoice_id},
                )

            if customer and customer.get("name"):
                customer_id = self._party_id("customer", customer)
                self._upsert_party("Customer", customer_id, customer)
                self._execute_cypher(
                    """
                    MATCH (i:Invoice {id: $invoice_id}), (c:Customer {id: $customer_id})
                    MERGE (i)-[:BILLED_TO]->(c)
                    """,
                    {"invoice_id": invoice_id, "customer_id": customer_id},
                )

            line_items = data.get("line_items", [])
            if isinstance(line_items, list):
                for idx, item in enumerate(line_items):
                    if not isinstance(item, dict):
                        continue
                    item_id = f"{invoice_id}:item:{idx}"
                    self._execute_cypher(
                        """
                        MERGE (li:LineItem {id: $id})
                        SET
                            li.description = coalesce($description, li.description),
                            li.quantity = coalesce($quantity, li.quantity),
                            li.unit_price = coalesce($unit_price, li.unit_price),
                            li.amount = coalesce($amount, li.amount),
                            li.sku = coalesce($sku, li.sku),
                            li.unit = coalesce($unit, li.unit)
                        """,
                        {
                            "id": item_id,
                            "description": self._nullable_string(item.get("description")),
                            "quantity": self._nullable_number(item.get("quantity")),
                            "unit_price": self._nullable_number(item.get("unit_price")),
                            "amount": self._nullable_number(item.get("amount")),
                            "sku": self._nullable_string(item.get("sku")),
                            "unit": self._nullable_string(item.get("unit")),
                        },
                    )
                    self._execute_cypher(
                        """
                        MATCH (i:Invoice {id: $invoice_id}), (li:LineItem {id: $line_item_id})
                        MERGE (i)-[:CONTAINS]->(li)
                        """,
                        {"invoice_id": invoice_id, "line_item_id": item_id},
                    )

            tax_details = data.get("tax_details", [])
            if isinstance(tax_details, list):
                for idx, tax in enumerate(tax_details):
                    if not isinstance(tax, dict):
                        continue
                    tax_id = f"{invoice_id}:tax:{idx}"
                    self._execute_cypher(
                        """
                        MERGE (t:Tax {id: $id})
                        SET
                            t.tax_type = coalesce($tax_type, t.tax_type),
                            t.rate = coalesce($rate, t.rate),
                            t.amount = coalesce($amount, t.amount)
                        """,
                        {
                            "id": tax_id,
                            "tax_type": self._nullable_string(tax.get("tax_type")),
                            "rate": self._nullable_number(tax.get("rate")),
                            "amount": self._nullable_number(tax.get("amount")),
                        },
                    )
                    self._execute_cypher(
                        """
                        MATCH (i:Invoice {id: $invoice_id}), (t:Tax {id: $tax_id})
                        MERGE (i)-[:HAS_TAX]->(t)
                        """,
                        {"invoice_id": invoice_id, "tax_id": tax_id},
                    )

            self.conn.execute("COMMIT")
            return True
        except Exception:
            try:
                self.conn.execute("ROLLBACK")
            except Exception:
                pass
            logger.exception(f"Failed to ingest invoice (id={invoice_id}) from {file_path}")
            return False

    def ingest_directory(self, folder_path: Path):
        """Recursively scan directory for result.json files and ingest them."""
        count = 0
        logger.info(f"Scanning {folder_path} for result.json files...")
        
        for file_path in folder_path.rglob("result.json"):
            if self.ingest_json(file_path):
                count += 1

        logger.info(f"Graph ingestion complete. Processed {count} files.")
        return count

    def _upsert_document(
        self,
        document_id: str,
        result_path: Path,
        source_file: Optional[Path],
        output_folder: Path,
    ) -> None:
        self._execute_cypher(
            """
            MERGE (d:Document {id: $id})
            SET
                d.result_path = $result_path,
                d.output_folder = $output_folder,
                d.source_file = $source_file
            """,
            {
                "id": document_id,
                "result_path": str(result_path),
                "output_folder": str(output_folder),
                "source_file": str(source_file) if source_file else "",
            },
        )

    def _upsert_party(self, label: str, party_id: str, party: dict[str, Any]) -> None:
        address = party.get("address") if isinstance(party.get("address"), dict) else {}
        query = f"""
            MERGE (p:{label} {{id: $id}})
            SET
                p.name = coalesce($name, p.name),
                p.normalized_name = coalesce($normalized_name, p.normalized_name),
                p.tax_id = coalesce($tax_id, p.tax_id),
                p.email = coalesce($email, p.email),
                p.phone = coalesce($phone, p.phone),
                p.street = coalesce($street, p.street),
                p.city = coalesce($city, p.city),
                p.state = coalesce($state, p.state),
                p.postal_code = coalesce($postal_code, p.postal_code),
                p.country = coalesce($country, p.country)
            """
        self._execute_cypher(
            query,
            {
                "id": party_id,
                "name": self._nullable_string(party.get("name")),
                "normalized_name": self._normalize_text(party.get("name")) or None,
                "tax_id": self._nullable_string(party.get("tax_id")),
                "email": self._normalize_email(party.get("email")) or None,
                "phone": self._nullable_string(party.get("phone")),
                "street": self._nullable_string(address.get("street")),
                "city": self._nullable_string(address.get("city")),
                "state": self._nullable_string(address.get("state")),
                "postal_code": self._nullable_string(address.get("postal_code")),
                "country": self._nullable_string(address.get("country")),
            },
        )

    def _invoice_id(
        self,
        invoice: dict[str, Any],
        vendor: dict[str, Any],
        file_path: Path,
        page: Optional[int],
        record_index: int,
    ) -> str:
        vendor_key = self._party_id("vendor", vendor) if vendor else "vendor:unknown"
        invoice_number = self._normalize_text(invoice.get("invoice_number"))
        if invoice_number:
            return self._hash_id("invoice", vendor_key, invoice_number)

        return self._hash_id(
            "invoice",
            vendor_key,
            self._string(invoice.get("invoice_date")),
            self._string(invoice.get("total")),
            str(file_path.resolve()),
            str(page or record_index),
        )

    def _party_id(self, party_type: str, party: dict[str, Any]) -> str:
        tax_id = self._normalize_text(party.get("tax_id"))
        if tax_id:
            return self._hash_id(party_type, "tax", tax_id)

        email = self._normalize_email(party.get("email"))
        if email:
            return self._hash_id(party_type, "email", email)

        address = party.get("address") if isinstance(party.get("address"), dict) else {}
        return self._hash_id(
            party_type,
            "name",
            self._normalize_text(party.get("name")),
            self._normalize_text(address.get("postal_code")),
            self._normalize_text(address.get("country")),
        )

    def _document_id(self, result_path: Path, source_file: Optional[Path]) -> str:
        if source_file and source_file.exists():
            return self._file_hash(source_file)

        original_files = sorted(
            p for p in result_path.parent.glob("original_*") if p.is_file()
        )
        if original_files:
            return self._file_hash(original_files[0])

        return self._hash_id("document", str(result_path.resolve()))

    def _file_hash(self, path: Path) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        return f"document:{digest.hexdigest()}"

    def _hash_id(self, prefix: str, *parts: str) -> str:
        digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
        return f"{prefix}:{digest}"

    def _normalize_text(self, value: Any) -> str:
        text = self._string(value).lower()
        text = re.sub(r"[^a-z0-9]+", " ", text)
        return " ".join(text.split())

    def _normalize_email(self, value: Any) -> str:
        return self._string(value).strip().lower()

    def _string(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    def _number(self, value: Any) -> float:
        if value in (None, ""):
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _nullable_string(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        s = str(value).strip()
        return s if s else None

    def _nullable_number(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

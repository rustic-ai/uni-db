
import os

file_path = "../src/query/parser.rs"

with open(file_path, "r") as f:
    content = f.read()

# 1. Add Delete variant to Clause enum
old_clause = "    Remove(RemoveClause),"
new_clause = "    Remove(RemoveClause),
    Delete(DeleteClause),"
if "Delete(DeleteClause)" not in content:
    content = content.replace(old_clause, new_clause)

# 2. Define DeleteClause struct
old_struct = "#[derive(Debug, Clone, PartialEq)]\npub struct CreateClause {"
new_struct = "#[derive(Debug, Clone, PartialEq)]\npub struct DeleteClause {
    pub items: Vec<Expr>,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq)]\npub struct CreateClause {"
if "struct DeleteClause" not in content:
    content = content.replace(old_struct, new_struct)

# 3. Add parsing logic in loop
old_logic = "            if self.peek_keyword(\"REMOVE\") {
                self.advance();
                clauses.push(Clause::Remove(self.parse_remove()?));
                continue;
            }"
new_logic = "            if self.peek_keyword(\"REMOVE\") {
                self.advance();
                clauses.push(Clause::Remove(self.parse_remove()?));
                continue;
            }

            if self.peek_keyword(\"DELETE\") {
                self.advance();
                clauses.push(Clause::Delete(self.parse_delete(false)?));
                continue;
            }

            if self.peek_keyword(\"DETACH\") {
                self.advance();
                self.expect_keyword(\"DELETE\")?;
                clauses.push(Clause::Delete(self.parse_delete(true)?));
                continue;
            }"
if "self.peek_keyword(\"DELETE\")" not in content:
    content = content.replace(old_logic, new_logic)

# 4. Add parse_delete helper
old_helper_anchor = "        Ok(RemoveClause { items })
    }

    fn parse_properties(&mut self) -> Result<Vec<(String, Expr)>> {"
new_helper = "        Ok(RemoveClause { items })
    }

    fn parse_delete(&mut self, detach: bool) -> Result<DeleteClause> {
        let mut items = Vec::new();
        loop {
            items.push(self.parse_expr()?);
            if !self.consume_token(Token::Comma) {
                break;
            }
        }
        Ok(DeleteClause { items, detach })
    }

    fn parse_properties(&mut self) -> Result<Vec<(String, Expr)>> {"

if "fn parse_delete" not in content:
    content = content.replace(old_helper_anchor, new_helper)

with open(file_path, "w") as f:
    f.write(content)

print("Successfully updated parser.rs")

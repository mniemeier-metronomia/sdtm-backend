from sqlalchemy import (
    create_engine, Column, Integer, Text, Boolean, ForeignKey,
    DateTime, ARRAY, Table, Date, Index, UniqueConstraint
)
from sqlalchemy.orm import relationship, sessionmaker, declarative_base, foreign
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func, and_, select
from constants import DATABASE_URL

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)

Base = declarative_base()

# ----------------------------
# Many-to-many associations
# ----------------------------

mapping_schema_domain = Table( "mapping_schema_domain", Base.metadata, Column("mapping_schema_id", Integer, ForeignKey("mapping_schema.id", ondelete="CASCADE"), primary_key=True), Column("sdtm_domain_id", Integer, ForeignKey("sdtm_domain.id", ondelete="CASCADE"), primary_key=True))

# ----------------------------
# Core Models
# ----------------------------

class Project(Base):
    __tablename__ = "project"
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    source_files = relationship("SourceFile", back_populates="project")
    mapping_schemas = relationship("MappingSchema", back_populates="project")


class SourceFile(Base):
    __tablename__ = "source_file"
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("project.id", ondelete="CASCADE"), nullable=False)
    name = Column(Text, nullable=False)
    description = Column(Text)
    uploaded_at = Column(DateTime, server_default=func.now())
    content_type = Column(Text)
    file_oid = Column(Integer)
    num_rows = Column(Integer)
    num_columns = Column(Integer)
    key_columns = Column(ARRAY(Text))
    included_columns = Column(ARRAY(Text))
    table_created_at = Column(DateTime, server_default=func.now())

    # relationships
    project = relationship("Project", back_populates="source_files")
    source_columns = relationship("SourceColumn", back_populates="source_file", cascade="all, delete-orphan")
    source_datas   = relationship("SourceData",   back_populates="source_file", cascade="all, delete-orphan")

    mapping_schema_links = relationship("MappingSchemaSourceFile", back_populates="source_file")
    mapping_schemas = relationship(
        "MappingSchema",
        secondary=lambda: MappingSchemaSourceFile.__table__,
        back_populates="source_files",
        viewonly=True
    )


class SourceColumn(Base):
    __tablename__ = "source_column"
    id = Column(Integer, primary_key=True)
    source_file_id = Column(Integer, ForeignKey("source_file.id", ondelete="CASCADE"), nullable=False)
    name = Column(Text, nullable=False)
    data_type = Column(Text)
    ordinal = Column(Integer)
    sample_values = Column(ARRAY(Text))
    variant = Column(Text, default="raw")
    created_at = Column(DateTime, server_default=func.now())
    description = Column(Text)

    source_file = relationship("SourceFile", back_populates="source_columns")


class SourceData(Base):
    __tablename__ = "source_data"
    id = Column(Integer, primary_key=True)
    source_file_id = Column(Integer, ForeignKey("source_file.id", ondelete="CASCADE"), nullable=False)
    row_index = Column(Integer, nullable=False)
    column_name = Column(Text, nullable=False)
    value = Column(Text)
    variant = Column(Text, default="raw")
    source_file = relationship("SourceFile", back_populates="source_datas")


class MappingSchema(Base):
    __tablename__ = "mapping_schema"
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("project.id", ondelete="CASCADE"), nullable=False)
    sdtm_standard_id = Column(Integer, ForeignKey("sdtm_standard.id", ondelete="SET NULL"))
    name = Column(Text, nullable=False)
    version = Column(Text)
    status = Column(Text, nullable=False, default="draft")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # relationships
    project = relationship("Project", back_populates="mapping_schemas")
    sdtm_standard = relationship("SDTMStandard")
    source_file_links = relationship("MappingSchemaSourceFile", back_populates="mapping_schema", cascade="all, delete-orphan", passive_deletes=True)

    # convenient view-only many-to-many
    source_files = relationship("SourceFile", secondary=lambda: MappingSchemaSourceFile.__table__, back_populates="mapping_schemas", viewonly=True)
    domains = relationship("SDTMDomain", secondary=mapping_schema_domain, back_populates="mapping_schemas")


class MappingSchemaSourceFile(Base):
    __tablename__ = "mapping_schema_source_file"

    mapping_schema_id = Column(Integer, ForeignKey("mapping_schema.id", ondelete="CASCADE"), primary_key=True)
    source_file_id = Column(Integer, ForeignKey("source_file.id", ondelete="CASCADE"), primary_key=True)
    status = Column(Text, nullable=False, default="not_started")
    notes = Column(Text)
    mapping_json = Column(JSONB, nullable=False, server_default="{}")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # relationships
    mapping_schema = relationship("MappingSchema", back_populates="source_file_links")
    source_file = relationship("SourceFile", back_populates="mapping_schema_links")


class SDTMData(Base):
    __tablename__ = "sdtm_data"
    id = Column(Integer, primary_key=True)
    domain = Column(Text, nullable=False)
    row_index = Column(Integer, nullable=False)
    column_name = Column(Text, nullable=False)
    value = Column(Text)
    source_file_id = Column(Integer, ForeignKey("source_file.id", ondelete="CASCADE"), nullable=True)
    mapping_schema_id = Column(Integer, ForeignKey("mapping_schema.id", ondelete="CASCADE"), nullable=False)


class SDTMColumn(Base):
    __tablename__ = "sdtm_column"
    id = Column(Integer, primary_key=True)
    domain = Column(Text, nullable=False)
    name = Column(Text, nullable=False)
    data_type = Column(Text)
    ordinal = Column(Integer)
    source_file_id = Column(Integer, ForeignKey("source_file.id", ondelete="CASCADE"), nullable=False)
    mapping_schema_id = Column(Integer, ForeignKey("mapping_schema.id", ondelete="CASCADE"), nullable=False)


class SDTMStandard(Base):
    __tablename__ = "sdtm_standard"
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)     # e.g., "SDTMIG"
    version = Column(Text, nullable=False)  # e.g., "3.4"
    description = Column(Text)
    created_at = Column(DateTime, server_default=func.now())
    domains = relationship("SDTMDomain", back_populates="standard")


class SDTMCodelist(Base):
    __tablename__ = "sdtm_codelist"
    id = Column(Integer, primary_key=True)
    nci_code = Column(Text, nullable=False)              # e.g., "C66769"
    name = Column(Text, nullable=False)                  # e.g., "Severity/Intensity Scale for Adverse Events"
    extensible = Column(Boolean)                         # True/False/None if blank
    standard_name = Column(Text, nullable=True)          # e.g., "SDTM CT"
    standard_date = Column(Date, nullable=True)          # e.g., 2025-03-28
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("nci_code", "standard_date", name="uq_codelist_code_date"),
        Index("ix_codelist_code", "nci_code"),
        Index("ix_codelist_std_date", "standard_date"),
    )

    terms = relationship("SDTMCodelistTerm", back_populates="codelist", cascade="all, delete")


class SDTMCodelistTerm(Base):
    __tablename__ = "sdtm_codelist_term"
    id = Column(Integer, primary_key=True)
    codelist_id = Column(Integer, ForeignKey("sdtm_codelist.id", ondelete="CASCADE"), nullable=False)

    nci_term_code = Column(Text, nullable=False)         # e.g., "C41338"
    submission_value = Column(Text, nullable=False)      # e.g., "MILD"
    synonyms = Column(Text)                              # e.g., "1; Grade 1"
    definition = Column(Text)                            # CDISC Definition
    preferred_term = Column(Text)                        # NCI Preferred Term
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("codelist_id", "submission_value", name="uq_term_value_per_codelist"),
        Index("ix_term_codelist", "codelist_id"),
        Index("ix_term_value", "submission_value"),
        Index("ix_term_nci_code", "nci_term_code"),
    )

    codelist = relationship("SDTMCodelist", back_populates="terms")



class SDTMDomain(Base):
    __tablename__ = "sdtm_domain"
    id = Column(Integer, primary_key=True)
    standard_id = Column(Integer, ForeignKey("sdtm_standard.id", ondelete="CASCADE"), nullable=False)
    name = Column(Text, nullable=False)          # e.g., "AG", "VS", "AE"
    label = Column(Text)
    description = Column(Text)
    sdtm_class = Column(Text)                    # from "Class" col
    structure = Column(Text)                     # from "Structure" col

    #relationships
    standard = relationship("SDTMStandard", back_populates="domains")
    variables = relationship("SDTMVariable", back_populates="domain", cascade="all, delete")
    mapping_schemas = relationship(
        "MappingSchema",
        secondary=mapping_schema_domain,
        back_populates="domains"
    )


class SDTMVariable(Base):
    __tablename__ = "sdtm_variable"
    id = Column(Integer, primary_key=True)
    domain_id = Column(Integer, ForeignKey("sdtm_domain.id", ondelete="CASCADE"), nullable=False)
    name = Column(Text, nullable=False)                 # e.g., VSTESTCD
    label = Column(Text)
    data_type = Column(Text)                            # "Char"/"Num" per IG
    required = Column(Boolean, default=False)           # you already have: keep it (maps from Core=="Req")
    codelist = Column(Text)
    role = Column(Text)
    variable_order = Column(Integer)                    # from "Variable Order"
    core = Column(Text)                                 # Core: Req/Exp/Perm/Opt (store as text for now)
    described_value_domain = Column(Text)               # "Described Value Domain(s)"
    value_list = Column(Text)                           # "Value List"
    cdisc_notes = Column(Text)                          # "CDISC Notes"

    # relationships
    domain = relationship("SDTMDomain", back_populates="variables")

    codelist_entries = relationship(
        "SDTMCodelist",
        primaryjoin=foreign(codelist) == SDTMCodelist.nci_code,
        viewonly=True,
        lazy="selectin",
    )





Base.metadata.create_all(bind=engine)

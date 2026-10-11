Here's the full inventory based on the [What's New in Python 3.11](https://docs.python.org/3/whatsnew/3.11.html) notes, plus a few 3.11 changes that are documented elsewhere (asyncio docs) and the packaging and tooling cleanup that follows from a 3.11 floor. Features that arrived in 3.10 or earlier (`match`, `X | Y` unions, `ParamSpec`, parenthesized context managers) were already available to you, so they aren't listed.

---

## 1. Project/tooling changes (do these first)

- **Metadata:** set `requires-python = ">=3.11"`, remove the `Programming Language :: Python :: 3.10` classifier, and drop 3.10 from your CI matrix, tox/nox envs, and cibuildwheel `build` selectors.
- **Conditional dependencies to delete:** `tomli; python_version < "3.11"`, `exceptiongroup; python_version < "3.11"`, `async-timeout; python_version < "3.11"`, and possibly `typing_extensions` (see §4 for what still needs it).
- **Linters/type checkers:** set Ruff `target-version = "py311"`, Black `target-version = ["py311"]`, mypy `python_version = "3.11"`, and pyright `pythonVersion = "3.11"`. Ruff's pyupgrade rules will then auto-fix much of this list, for example UP036 (dead `sys.version_info` blocks), UP017 (`datetime.UTC`), and UP041 (`asyncio.TimeoutError` → `TimeoutError`).
- **Dead version checks:** every `if sys.version_info >= (3, 11):` is now always true, so delete the `else` branches.

## 2. New syntax you can use

- **`except*` and exception groups:** `ExceptionGroup` and `BaseExceptionGroup` are now builtins, and the new `except*` clause matches subgroups of an exception group. The builtin types ExceptionGroup and BaseExceptionGroup make it possible to group exceptions and raise them together, and the new except* syntax generalizes except to match subgroups of exception groups. This replaces the `exceptiongroup` backport and its `catch()` helper.
  ```python
  try:
      async with asyncio.TaskGroup() as tg: ...
  except* ValueError as eg: ...
  except* OSError as eg: ...
  ```
- **Starred unpacking in `for`:** Starred unpacking expressions can now be used in for statements. For example, `for x in *a, *b:`.
- **Nested async comprehensions:** Asynchronous comprehensions are now allowed inside comprehensions in asynchronous functions. Outer comprehensions implicitly become asynchronous in this case.
- **`z` format option:** A "z" option was added to the format specification that coerces negative to positive zero after rounding to the format precision (PEP 682). For example, `f"{-0.0001:z.2f}"` gives `'0.00'` instead of `'-0.00'`. This replaces manual `+ 0.0` or `abs()` hacks.
- **Regex atomic groups and possessive quantifiers:** Atomic grouping ((?>...)) and possessive quantifiers (*+, ++, ?+, {m,n}+) are now supported in regular expressions. If you depended on the third-party `regex` module only for these, you can switch back to `re`.

## 3. New builtins and exception features

- **`BaseException.add_note()`:** The add_note() method is added to BaseException. It can be used to enrich exceptions with context information that is not available at the time when the exception is raised. The added notes appear in the default traceback. This replaces patterns like re-raising with a modified message or wrapping in a custom exception just to add context.
- **`sys.exception()`:** this returns the active exception instance (equivalent to sys.exc_info()[1]).
- **`TimeoutError` unification:** `asyncio.TimeoutError` became an alias of the builtin `TimeoutError` in 3.11 (per the asyncio docs). You can now catch plain `TimeoutError` everywhere.

## 4. Typing: backports from `typing_extensions` you can drop

You can import all of these directly from `typing`:

- **`Self`** (PEP 673): this replaces the `TypeVar("T", bound="MyClass")` pattern for fluent methods, `__enter__`, and alternate-constructor classmethods.
- **`LiteralString`** (PEP 675): useful for SQL and shell-injection-sensitive APIs.
- **`Required` / `NotRequired`** (PEP 655): previously, marking individual `TypedDict` keys this way was only possible using inheritance. You can collapse those `_Base(total=False)` split hierarchies.
- **`TypeVarTuple` / `Unpack`** (PEP 646): variadic generics, for example array shapes.
- **`dataclass_transform`** (PEP 681): important if your library has a dataclass-like decorator, base class, or metaclass (ORM models, config classes, and similar).
- **`Never`, `assert_never`, `reveal_type`, `assert_type`.**
- **`get_overloads` / `clear_overloads`.**
- **Generic `TypedDict` and generic `NamedTuple`:** `class Box(TypedDict, Generic[T])` and the equivalent for `NamedTuple` now work at runtime.
- **Subclassing `typing.Any`:** This is useful for avoiding type checker errors related to highly dynamic class, such as mocks.
- **Other runtime behavior you can rely on:** `@final` now sets `__final__`, `Protocol` subclasses keep their `__init__`, and `get_type_hints()` no longer wraps `None`-default parameters in `Optional`.

Keep `typing_extensions` if you use anything newer than 3.11, such as `override` (3.12), `TypeIs`, `ReadOnly`, `deprecated`, and TypeVar defaults (3.13), or its newer `TypedDict` features.

## 5. Standard library: replacements for third-party packages or hand-rolled code

**New modules**
- **`tomllib`:** this replaces `tomli` for reading TOML. It can't write TOML, so keep `tomli-w` if you need that.
- **`wsgiref.types`:** WSGI-specific types for static type checking.

**asyncio**
- **`asyncio.TaskGroup`:** For new code this is recommended over using create_task() and gather() directly.
- **`asyncio.timeout()`:** this replaces the `async-timeout` package and most `wait_for` usage.
- **New classes:** `asyncio.Runner`, `asyncio.Barrier`, and `BrokenBarrierError`.
- **New methods and options:** `StreamWriter.start_tls()`, `loop.sock_sendto`, `sock_recvfrom`, and `sock_recvfrom_into`, `Task.cancelling()` and `Task.uncancel()`, and `create_connection(all_errors=True)`.

**enum**
- **`StrEnum` and `ReprEnum`:** these replace `class Foo(str, Enum)` and the `StrEnum` backport packages.
- **New helpers:** `verify()` with `EnumCheck`, `member()` and `nonmember()`, `enum.property`, `global_enum`, and `Flag` boundaries via `FlagBoundary`.
- **`Flag` behavior:** `Flag` now supports `len()`, iteration, and `in`.
- **Rename:** `EnumMeta` was renamed to `EnumType`, and the old name remains as an alias.

**datetime**
- **`datetime.UTC`:** an alias for `timezone.utc`.
- **Full ISO 8601 parsing:** `fromisoformat()` can now be used to parse most ISO 8601 formats. That includes the `Z` suffix, so you can drop `.replace("Z", "+00:00")` hacks and many `dateutil.parser.isoparse` uses.

**Smaller helpers**
- **`contextlib.chdir()`:** this replaces home-grown `cwd` context managers. It is not thread-safe.
- **`hashlib.file_digest()`:** this replaces manual chunked-read hashing loops.
- **`operator.call`, `math.exp2`, `math.cbrt`:** new functions.
- **`math.nan` always available:** no more fallback needed.
- **`logging.getLevelNamesMapping()`:** this replaces reaching into the private `logging._nameToLevel`.
- **`locale.getencoding()`:** gets the current locale encoding.
- **`warnings.catch_warnings()` options:** it now accepts arguments for simplefilter(), providing a more concise way to locally ignore warnings or convert them to errors, for example `with warnings.catch_warnings(action="ignore"):`.
- **`functools.singledispatch` with unions:** it now supports types.UnionType and typing.Union as annotations to the dispatch argument, so you can merge duplicated `.register` stubs.
- **`inspect`:** new `getmembers_static()` and `ismethodwrapper()`, and frame info now includes column positions.
- **`string.Template`:** new `get_identifiers()` and `is_valid()` methods.
- **`socket.create_connection(all_errors=True)`:** raises an `ExceptionGroup` with every connection error instead of just the last one.
- **`zipfile`:** new `ZipFile.mkdir()`, `Path.stem`, `Path.suffix`, and `Path.suffixes`, plus a `metadata_encoding` option.
- **`tempfile.SpooledTemporaryFile`:** it now fully implements the `BufferedIOBase`/`TextIOBase` interface, so you can remove wrapper workarounds.
- **`sqlite3`:**
  - New `Connection` methods: `blobopen()`, `serialize()` and `deserialize()`, `setlimit()` and `getlimit()`, and `create_window_function()`.
  - Exceptions now carry `sqlite_errorcode` and `sqlite_errorname`.
  - `set_authorizer(None)` now disables the authorizer.
- **`fractions.Fraction`:** it accepts underscores in strings like `"1_000/3"` and implements `__int__`.
- **`unittest`:** new `enterContext()`, `enterClassContext()`, `enterAsyncContext()`, and `enterModuleContext()`. These replace `addCleanup(cm.__exit__, ...)` boilerplate.

## 6. Behavior changes you can now assume (and should audit for)

- **Enum `format()` changed:** Changed Enum.__format__() (the default for format(), str.format() and f-strings) to always produce the same result as Enum.__str__(): for enums inheriting from ReprEnum it will be the member's value; for all other enums it will be the enum and member name (e.g. Color.RED). This means `f"{x}"` on a `(str, Enum)` member now gives `Color.RED`, not `"red"`. Switch those enums to `StrEnum`. Relatedly, `IntEnum` and `IntFlag` now `str()` to their value.
- **`Flag` aliases:** only power-of-two values are canonical. Composite values are treated as aliases, and inverted flags are coerced to positive values.
- **Non-context-manager error type:** `with` / `async with` on an object that isn't a context manager now raises `TypeError` instead of `AttributeError`. Update any `except AttributeError` around that.
- **`object.__getstate__()` exists:** This change has an unintended side effect: It trips up a small minority of existing Python projects not expecting object.__getstate__() to exist. Remove any `hasattr(obj, "__getstate__")` checks in custom pickle/copy code.
- **Stricter dataclass defaults:** the check now rejects any unhashable default, not just `list`, `dict`, and `set`.
- **`random`:** `random.sample()` requires a sequence (sets no longer accepted), and `random.shuffle()` lost its `random=` parameter.
- **`open()` mode `'U'` is gone.**
- **Regex inline flags:** global inline flags like `(?i)` must appear at the start of the pattern.
- **Removed APIs:** Removed the @asyncio.coroutine decorator enabling legacy generator-based coroutines. Also removed: `binhex`, `inspect.getargspec`, `inspect.formatargspec`, the `gettext.l*gettext` functions, and the `__getitem__` methods on `FileInput`, `wsgiref` `FileWrapper`, and `pulldom` `DOMEventStream`. Delete any compat branches that referenced these.
- **`pathlib` glob with trailing separator:** `Path.glob()` and `rglob()` with a trailing separator now return only directories.
- **Cheap `try`:** "Zero-cost" exceptions are implemented, eliminating the cost of try statements when no exception is raised. Micro-optimizations that hoisted `try` out of hot loops are no longer needed.

## 7. Deprecations announced in 3.11 to migrate off now

Each of these has a replacement that works on 3.11:

- **`importlib.resources` legacy functions:** `open_text`, `read_text`, `path`, `contents`, `is_resource`, `open_binary`, and `read_binary`. Use `files()` instead.
- **`locale`:** `getdefaultlocale()` and `resetlocale()`.
- **`typing.Text`:** use `str`.
- **Keyword-argument `TypedDict` construction:** for example `TypedDict("X", a=int)`.
- **`unittest` helpers:** `makeSuite`, `findTestCases`, and `getTestCaseNames`. Use `TestLoader` methods.
- **Returning non-`None` from test methods.**
- **Chained `classmethod` descriptors:** for example `classmethod` wrapping `property`.
- **`int()` delegating to `__trunc__`:** implement `__int__` or `__index__` instead.
- **Octal escapes above `\377`** in string and bytes literals.
- **PEP 594 "dead battery" modules:** `cgi`, `crypt`, `telnetlib`, `imghdr`, `pipes`, `uu`, and others. All were removed in 3.13.
- **`sre_*` modules, `lib2to3`, and `configparser.LegacyInterpolation`.**

## 8. C extensions (if your library has any)

- **Limited API / abi3:** you can target `Py_LIMITED_API 0x030B0000`, which brings Py_buffer and APIs are now part of the limited API and the stable ABI. That means buffer-protocol extensions can now ship a single `cp311-abi3` wheel.
- **New C APIs** (drop `pythoncapi_compat` shims for them):
  - `PyType_GetName` and `PyType_GetQualName`
  - `PyType_GetModuleByDef`
  - `PyFloat_Pack2/4/8` and `PyFloat_Unpack2/4/8`
  - `PyFrame_GetBuiltins`, `GetGenerator`, `GetGlobals`, and `GetLasti`
  - `PyErr_GetHandledException` and `PyErr_SetHandledException`
  - `PyThreadState_EnterTracing` and `PyThreadState_LeaveTracing`
  - `Py_Version`
- **`PyFrameObject` and `PyCodeObject` fields** are no longer directly accessible. Use the getter functions instead.
- **Limited-API include changes:** when targeting the limited API at 3.11 or later, `Python.h` no longer includes `<stdlib.h>`, `<stdio.h>`, `<errno.h>`, or `<string.h>`, so include them explicitly. The macros converted to static inline functions also no longer auto-cast their arguments.
- **Trashcan macros:** replace `Py_TRASHCAN_SAFE_BEGIN/END` with `Py_TRASHCAN_BEGIN/END`.

---

The highest-value items for most libraries are dropping `tomli`, `exceptiongroup`, and `async-timeout`, using `Self` and `dataclass_transform`, adopting `TaskGroup` and `timeout`, auditing `(str, Enum)` formatting, and deleting dead version branches with Ruff's UP rules.

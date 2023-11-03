use std::{collections::HashSet, process::Command};

use axum::{extract::Path, response::Html, Json, Router};
use chrono::Utc;
use runner::{DefRunner, Runner};
// use runner::{local::hash_dag, DefRunner, Runner};
use serde_json::{json, Value};
use server::{_get_dags, _get_edges, _get_options, _get_tasks, db::Db, DAGS_DIR, _run, catchup::catchup, scheduler::scheduler};
// use task::task::Task;

use axum::routing::get;
use task::task::Task;


async fn ping() -> &'static str {
    "pong"
}

async fn home() -> Html<String> {
    let res = "
    <!DOCTYPE html>
        <html>
        <body>
    "
    .to_string()
        + &_get_dags()
            .iter()
            .map(|f| format!("<a href=\"/runs/{f}\">{f}</a>"))
            .collect::<Vec<String>>()
            .join(" ")
        + "
    </body>
    </html>";

    Html(res.to_string())
}

async fn get_runs(Path(dag_name): Path<String>) -> Html<String> {
    let res = "
    <!DOCTYPE html>
        <html>
        <body>
    "
    .to_string()
        + &Db::get_runs(&dag_name)
            .await
            .iter()
            .map(|f| format!("<a href=\"/graph/{dag_name}/{f}\">{f}</a>"))
            .collect::<Vec<String>>()
            .join(" ")
        + "
    </body>
    </html>";

    Html(res.to_string())
}

async fn get_options(Path(dag_name): Path<String>) -> Json<Value> {
    _get_options(&dag_name).into()
}

async fn get_tasks(Path(dag_name): Path<String>) -> Json<Value> {
    _get_tasks(&dag_name).into()
}

async fn get_edges(Path(dag_name): Path<String>) -> Json<Value> {
    _get_edges(&dag_name).into()
}

async fn get_dags() -> Json<Value> {
    json!(_get_dags()).into()
}

async fn get_graph(Path(dag_name): Path<String>) -> Html<String> {
    let output = Command::new(format!("{DAGS_DIR}/{dag_name}"))
        .arg("graph")
        .output()
        .expect("failed to run");

    let result_raw = String::from_utf8_lossy(&output.stdout).to_string();
    format!(
        "
        <html>
        <body>
            {dag_name}
            <pre class=\"mermaid\">
                {result_raw}
            </pre>

            <script type=\"module\">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true }});
            </script>
        </body>
        </html>
    "
    )
    .into()
}

async fn get_run_graph(Path((dag_name, run_id)): Path<(String, usize)>) -> Html<String> {
    let runner = Db::new(&dag_name, &[], &HashSet::new());
    let completed = runner.is_completed(&run_id);
    let graph = runner.get_mermaid_graph(&run_id);

    axum::response::Html(
        format!(
            "
        <!DOCTYPE html>
        <html>
        <body>
            {dag_name}
            <pre class=\"mermaid\">
                {graph}
            </pre>"
        ) + r#"

            <label class="switch">
            <input type="checkbox" id="toggleRefresh">
            <span class="slider round"></span>
            </label>

            <!-- Display text for the auto-refresh status -->
            <span id="refreshStatus">Auto-Refresh (ON/OFF)</span>
            <div id="customTooltip" style="display: none; position: absolute; border: 1px solid black; background-color: white; padding: 10px;">
                <!-- Your custom content here -->
                Hovered Node Info
            </div>
        
            <style>
                /* The switch container */
                .switch {
                    position: relative;
                    display: inline-block;
                    width: 60px;
                    height: 34px;
                }
                
                /* The switch checkbox (hidden) */
                .switch input {
                    opacity: 0;
                    width: 0;
                    height: 0;
                }
                
                /* The slider */
                .slider {
                    position: absolute;
                    cursor: pointer;
                    top: 0;
                    left: 0;
                    right: 0;
                    bottom: 0;
                    background-color: #ccc;
                    transition: 0.4s;
                }
                
                .slider:before {
                    position: absolute;
                    content: "";
                    height: 26px;
                    width: 26px;
                    left: 4px;
                    bottom: 4px;
                    background-color: white;
                    transition: 0.4s;
                }
                
                /* When the checkbox is checked, change the slider's color and position */
                input:checked + .slider {
                    background-color: #2196F3;
                }
                
                input:checked + .slider:before {
                    transform: translateX(26px);
                }
                
                /* Rounded sliders */
                .slider.round {
                    border-radius: 34px;
                }
                
                .slider.round:before {
                    border-radius: 50%;
                }
            
            </style>
            <script type="module">
                import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
                mermaid.initialize({ 
                    startOnLoad: false,
                    securityLevel: 'loose',  // This may be required for some versions of Mermaid to allow certain content.
                    theme: 'default',
                });
                // mermaid.initialize({ startOnLoad: false });
                await mermaid.run({
                querySelector: '.mermaid',
                postRenderCallback: 
                function() {
                    const nodes = document.querySelectorAll('.mermaid .node');
                    // console.log("world");
                    // console.log(nodes);
                    nodes.forEach(node => {
                        // console.log("r");
                        node.addEventListener('mouseenter', function(event) {
                            const tooltip = document.getElementById('customTooltip');
                            console.log("hello");
                            // Set the content of the tooltip based on the hovered node. 
                            // For this example, I'm just using the node's text content.
                            tooltip.innerHTML = node.textContent;
                
                            tooltip.style.display = 'block';
                            tooltip.style.left = event.pageX + 'px';
                            tooltip.style.top = (event.pageY - tooltip.offsetHeight) + 'px';
                        });
                        // console.log("w");

                        node.addEventListener('mouseleave', function() {
                            const tooltip = document.getElementById('customTooltip');
                            tooltip.style.display = 'none';
                        });
                        // console.log("t");

                    });
                }
                });
            "#
            + &format!(
                "
                let isRefreshing = {};",
                !completed
            )
            + r#"
                const toggleSwitch = document.getElementById('toggleRefresh');
                toggleSwitch.checked = isRefreshing;

                const refreshStatus = document.getElementById('refreshStatus');

                const setSwitchText = () => {
                    refreshStatus.textContent = isRefreshing ? "Auto-Refresh (ON)" : "Auto-Refresh (OFF)";
                };

                const toggleRefresh = () => {
                    if (isRefreshing) {
                        clearTimeout(refreshTimeout);
                    } else {
                        scheduleRefresh();
                    }
                    isRefreshing = !isRefreshing;
                    setSwitchText();
                };

                toggleSwitch.addEventListener('change', toggleRefresh);

                let refreshTimeout;
                const scheduleRefresh = () => {
                    refreshTimeout = setTimeout(function() {
                        location.reload();
                    }, 5000);
                };

                setSwitchText();
                if (isRefreshing) {
                    scheduleRefresh();
                    toggleSwitch.checked = true; // Set the initial state of the toggle switch
                }            
            </script>
        </body>
        </html>
    "#,
    )
}

async fn run(Path(dag_name): Path<String>) {
    _run(&dag_name, Utc::now().into());
}

async fn run_local(Path(dag_name): Path<String>) {
    let nodes: Vec<Task> = serde_json::from_value(_get_tasks(&dag_name)).unwrap();
    let edges: HashSet<(usize, usize)> = serde_json::from_value(_get_edges(&dag_name)).unwrap();

    Db::new(&dag_name, &nodes, &edges).run_dag_local(1);
}

#[tokio::main]
async fn main() {
    Db::init_tables().await;

    let now = Utc::now();

    catchup(&now);
    scheduler(&now);

    let app = Router::new()
        .route("/ping", get(ping))
        .route("/", get(home))
        .route("/dags", get(get_dags))
        .route("/options/:dag_name", get(get_options))
        .route("/tasks/:dag_name", get(get_tasks))
        .route("/graph/:dag_name", get(get_graph))
        .route("/runs/:dag_name", get(get_runs))
        .route("/graph/:dag_name/:run_id", get(get_run_graph))
        .route("/edges/:dag_name", get(get_edges))
        .route("/run/:dag_name", get(run))
        .route("/run_local/:dag_name", get(run_local));

    axum::Server::bind(&"0.0.0.0:8000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

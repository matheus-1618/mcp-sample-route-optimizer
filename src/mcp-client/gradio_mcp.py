from dotenv import load_dotenv
import os
import asyncio
import json
import uuid
import boto3
import copy
import gradio as gr
import time
from PIL import Image
import io
import re
from InlineAgent.tools.mcp import MCPHttp, MCPStdio
from mcp import StdioServerParameters
from InlineAgent.action_group import ActionGroup, ActionGroups
from InlineAgent.agent import InlineAgent
from InlineAgent import AgentAppConfig
from InlineAgent.agent.process_roc import ProcessROC
from InlineAgent.observability import Trace
from langfuse import Langfuse

load_dotenv()

langfuse = Langfuse(
    debug=False  
)

PROMPT_INSTRUCTION = """
You are a specialized logistics assistant designed to help with location services, internet searches, database access, and data analysis. Your capabilities include:

<location_services>

    Calculate optimal routes between points using Amazon Location Service
    Find nearby points of interest (restaurants, gas stations, etc.)
    Determine which establishments are currently open
    Estimate travel times and distances
    Suggest alternative routes based on traffic conditions </location_services>

<internet_search>

    Search for current information and news from 2025
    Access and inspect specific websites when needed
    Research logistics trends, regulations, and market conditions
    Find information about shipping options and transportation providers
    Navigate to URLs for detailed information gathering </internet_search>

<database_access>

    Query the following DynamoDB tables:
        mcp-clientes (customer information)
        mcp-entregas (delivery details)
        mcp-pedidos (order information)
        mcp-produtos (product catalog)
        mcp-veiculos (vehicle fleet data)
    Analyze relationships between customers, orders, products, and deliveries
    Track vehicle availability and delivery capacity </database_access>

<code_interpreter>

    Analyze logistics data using Python
    Create visualizations with matplotlib
    Perform statistical analysis with numpy and scipy
    Process and transform data with pandas
    Generate insights from logistics datasets </code_interpreter>

For any request, I'll think step-by-step and clearly explain my reasoning process. 
I can help with finding optimal shipping routes, tracking deliveries, analyzing logistics data, 
providing up-to-date information on transportation options, 
calculating costs and timeframes, and visualizing data for better decision-making.
"""

def calculate_anthropic_costs(model_name, input_tokens, output_tokens, cache_read_tokens=0):
    """
    Calculate costs for Anthropic models based on their pricing
    
    Args:
        model_name: str - The name of the model
        input_tokens: int - Number of input tokens
        output_tokens: int - Number of output tokens
        cache_read_tokens: int - Number of cache read tokens (defaults to 0)
        
    Returns:
        dict - Dictionary with cost details
    """
    if "claude-3-5-sonnet" in model_name.lower():
        input_cost = input_tokens * 0.003 / 1000  
        output_cost = output_tokens * 0.015 / 1000  
        cache_cost = cache_read_tokens * 0.0003 / 1000  
    elif "claude-3-5-haiku" in model_name.lower():
        input_cost = input_tokens * 0.0008 / 1000  
        output_cost = output_tokens * 0.004 / 1000  
        cache_cost = cache_read_tokens * 0.00008 / 1000  
    elif "claude-3-7-sonnet" in model_name.lower():
        input_cost = input_tokens * 0.003 / 1000  
        output_cost = output_tokens * 0.015 / 1000  
        cache_cost = cache_read_tokens * 0.0003 / 1000  
    else:
        
        input_cost = input_tokens * 0.003 / 1000
        output_cost = output_tokens * 0.015 / 1000
        cache_cost = cache_read_tokens * 0.0003 / 1000
    
    total_cost = input_cost + output_cost + cache_cost
    
    return {
        "input": input_cost,
        "output": output_cost,
        "cache_read_input_tokens": cache_cost,
        "total": total_cost
    }

async def process_roc_event(tool, event, inlineSessionState):
    print(event["returnControl"])
    roc_trace = langfuse.trace(
        name="roc-processing",
        input={"event_type": "returnControl"}
    )
    
    try:
        inlineSessionState1 = await ProcessROC.process_roc(
            inlineSessionState=inlineSessionState,
            roc_event=event["returnControl"],
            tool_map=tool,
        )
        
        roc_trace.update(
            output={"status": "success"}
        )
        
        return inlineSessionState1
    except Exception as e:
        
        roc_trace.update(
            level="ERROR",
            status_message=str(e),
            output={"status": "error", "message": str(e)}
        )
        raise


async def process_single_iteration(generator):
    try:
        return await anext(generator)
    except StopAsyncIteration:
        return None


def extract_model_thoughts(text):
    patterns = [
        r"(?i)Estou (pensando|considerando|analisando|avaliando|refletindo sobre|processando|investigando) ([^\n\.]+)",
        r"(?i)Vou (buscar|pesquisar|procurar|consultar|investigar) ([^\n\.]+)",
        r"(?i)Preciso (encontrar|determinar|verificar|confirmar|checar) ([^\n\.]+)",
        r"(?i)(Primeiro|Inicialmente|Agora|Em seguida|Depois), (vou|irei|devo|preciso) ([^\n\.]+)",
        r"(?i)Minha (estratégia|abordagem|análise|pesquisa) ([^\n\.]+)",
        r"(?i)Para (responder|resolver|abordar) ([^\n\.]+)"
    ]
    
    thoughts = []
    for pattern in patterns:
        matches = re.findall(pattern, text)
        if matches:
            for match in matches:
                if isinstance(match, tuple):
                    thought = " ".join(match)
                else:
                    thought = match
                thoughts.append(thought)
    
    return thoughts

async def agent_process(message, model_name="us.anthropic.claude-3-5-sonnet-20241022-v2:0", config=None):
    if config is None:
        config = AgentAppConfig()
    
    trace = langfuse.trace(
        name="agent-query",
        input=message,
        user_id=str(uuid.uuid4()),  
        tags=["bedrock-agent"]
    )
    
    full_response = ""
    images = []
    traces = []
    model_thoughts = []
    
    traces.append({
        "action": "Iniciando processamento",
        "time": time.strftime("%H:%M:%S"),
        "details": f"Consulta: {message[:50]}..."
    })
    model_thoughts.append({
        "thought": "Analisando sua pergunta para determinar a melhor abordagem",
        "time": time.strftime("%H:%M:%S")
    })
    
    
    trace.event(
        name="processing-started",
        input={"query": message[:100]}
    )
    
    yield full_response, images, traces, model_thoughts
    
    
    mcp_init_span = trace.span(name="mcp-client-initialization")
    
    location_mcp_client = None
    internet_search_mcp_client = None
    dynamo_mcp_client = None
    
    traces.append({
        "action": "MCP Client inicializado",
        "time": time.strftime("%H:%M:%S")
    })
    model_thoughts.append({
        "thought": "Iniciando conexão aos MCP Servers",
        "time": time.strftime("%H:%M:%S")
    })
    yield full_response, images, traces, model_thoughts
    
    try:
        headers = {
            "Authorization": f"Bearer {config.BEARER_TOKEN}"
        }
        location_mcp_client = await MCPHttp.create(url=config.MCP_SSE_LOCATION_URL, headers = headers)
        internet_search_mcp_client = await MCPHttp.create(url=config.MCP_SSE_URL, headers = headers)
        dynamo_mcp_client = await MCPHttp.create(url=config.MCP_SSE_DYBAMO_URL, headers = headers)
        

        mcp_init_span.end(output={"status": "success", "clients": 3})
        
        action_group_span = trace.span(name="setup-action-groups")
        location_action_group = ActionGroup(
            name="LocationActionGroup",
            mcp_clients=[location_mcp_client],
        )
        internet_search_action_group = ActionGroup(
            name="InternetSearchActionGroup",
            mcp_clients=[internet_search_mcp_client],
        )
        dynamo_action_group = ActionGroup(
            name="DynamoActionGroup",
            mcp_clients=[dynamo_mcp_client],
        )
        action_groups = [
            location_action_group,
            internet_search_action_group,
            dynamo_action_group,
            {
                "name": "CodeInterpreter",
                "builtin_tools": {
                    "parentActionGroupSignature": "AMAZON.CodeInterpreter"
                },
            },
        ]
        
        
        action_group_span.end(output={"group_count": len(action_groups)})
        
        traces.append({
            "action": "Action groups configurados",
            "time": time.strftime("%H:%M:%S"),
            "details": "InternetSearchActionGroup, CodeInterpreter"
        })
        yield full_response, images, traces, model_thoughts
        
        
        agent_init_span = trace.span(name="agent-initialization")
        
        
        internet_agent = InlineAgent(
            foundation_model=model_name,
            
            
            
            instruction=PROMPT_INSTRUCTION,
            agent_name="location_search_agent",
            action_groups=action_groups,
        )
        
        agent_init_span.end(output={"model": model_name})
        
        traces.append({
            "action": "Agente inicializado",
            "time": time.strftime("%H:%M:%S"),
            "details": f"Modelo: {model_name}"
        })
        model_thoughts.append({
            "thought": f"Usando {model_name} para processar sua consulta",
            "time": time.strftime("%H:%M:%S")
        })
        yield full_response, images, traces, model_thoughts
        
        
        initial_invoke_span = trace.span(name="initial-invoke")
        response = await internet_agent.invoke(
            input_text=message,
            process_response=False,
            add_citation=True
        )
        initial_invoke_span.end()
        
        traces.append({
            "action": "Invocação inicial do agente",
            "time": time.strftime("%H:%M:%S")
        })
        model_thoughts.append({
            "thought": f"Formulando estratégia de busca para: '{message}'",
            "time": time.strftime("%H:%M:%S")
        })
        yield full_response, images, traces, model_thoughts
        
        
        session_id = str(uuid.uuid4())
        enable_trace = True
        session_state = {}
        inlineSessionState = copy.deepcopy(session_state)
        agent_answer = ""
        streaming_configurations = {"streamFinalResponse": False}
        add_citation = True
        truncate_response = None
        params = internet_agent.get_invoke_params()
        
        
        bedrock_agent_runtime = boto3.Session().client("bedrock-agent-runtime")
        
        
        total_input_tokens = 0
        total_output_tokens = 0
        total_llm_calls = 0
        total_cache_read_tokens = 0
        cite = None
        
        
        event_stream = response["completion"]
        
        traces.append({
            "action": "Processando stream de eventos",
            "time": time.strftime("%H:%M:%S"),
            "details": f"Session ID: {session_id}"
        })
        yield full_response, images, traces, model_thoughts
        
        
        orchestration_span = trace.span(name="orchestration-loop")
        
        while not agent_answer:
            
            bedrock_span = orchestration_span.span(name="bedrock-invoke")
            
            if inlineSessionState:
                bedrock_span.update(metadata={"has_session_state": True})
                response = bedrock_agent_runtime.invoke_inline_agent(
                    sessionId=session_id,
                    inputText=message,
                    enableTrace=enable_trace,
                    inlineSessionState=inlineSessionState,
                    streamingConfigurations=streaming_configurations,
                    **params
                )
                
                traces.append({
                    "action": "Invocação do agente com estado de sessão",
                    "time": time.strftime("%H:%M:%S")
                })
                yield full_response, images, traces, model_thoughts
            else:
                bedrock_span.update(metadata={"has_session_state": False})
                response = bedrock_agent_runtime.invoke_inline_agent(
                    sessionId=session_id,
                    inputText=message,
                    enableTrace=enable_trace,
                    streamingConfigurations=streaming_configurations,
                    **params
                )
                
                traces.append({
                    "action": "Invocação do agente sem estado de sessão",
                    "time": time.strftime("%H:%M:%S")
                })
                model_thoughts.append({
                    "thought": "Iniciando definição da ação a ser tomada.",
                    "time": time.strftime("%H:%M:%S")
                })
                yield full_response, images, traces, model_thoughts
            
            
            bedrock_span.end()
            
            inlineSessionState = copy.deepcopy(session_state)
            event_stream = response["completion"]
            
            
            stream_span = orchestration_span.span(name="process-event-stream")
            
            try:
                for event in event_stream:
                    
                    if "files" in event:
                        file_span = stream_span.span(name="process-files")
                        files_event = event["files"]
                        files_list = files_event["files"]
                        
                        file_span.update(metadata={"file_count": len(files_list)})
                        
                        traces.append({
                            "action": f"Recebendo {len(files_list)} arquivo(s)",
                            "time": time.strftime("%H:%M:%S")
                        })
                        model_thoughts.append({
                            "thought": f"Processando {len(files_list)} arquivo(s) com dados relevantes",
                            "time": time.strftime("%H:%M:%S")
                        })
                        yield full_response, images, traces, model_thoughts
                        
                        processed_files = 0
                        for idx, this_file in enumerate(files_list):
                            single_file_span = file_span.span(name=f"file-{idx}")
                            single_file_span.update(metadata={
                                "file_name": this_file.get("name", "unknown"),
                                "file_type": this_file.get("type", "unknown")
                            })
                            
                            file_bytes = this_file["bytes"]
                            file_name = this_file["name"]
                            file_type = this_file.get("type", "unknown")
                            
                            
                            directory_path = os.path.join(os.getcwd(), "output")
                            if not os.path.exists(directory_path):
                                try:
                                    os.makedirs(directory_path, exist_ok=True)
                                except OSError as e:
                                    error_msg = str(e)
                                    traces.append({
                                        "action": f"Erro ao criar diretório",
                                        "time": time.strftime("%H:%M:%S"),
                                        "details": error_msg
                                    })
                                    
                                    
                                    single_file_span.update(
                                        level="ERROR", 
                                        status_message=f"Directory creation error: {error_msg}"
                                    )
                                    
                                    yield full_response, images, traces, model_thoughts
                                    continue
                            
                            session_dir = os.path.join(directory_path, str(session_id))
                            if not os.path.exists(session_dir):
                                try:
                                    os.makedirs(session_dir, exist_ok=True)
                                except OSError as e:
                                    error_msg = str(e)
                                    traces.append({
                                        "action": f"Erro ao criar diretório de sessão",
                                        "time": time.strftime("%H:%M:%S"),
                                        "details": error_msg
                                    })
                                    
                                    
                                    single_file_span.update(
                                        level="ERROR", 
                                        status_message=f"Session directory error: {error_msg}"
                                    )
                                    
                                    yield full_response, images, traces, model_thoughts
                                    continue
                            
                            
                            file_path = os.path.join(session_dir, file_name)
                            with open(file_path, "wb") as f:
                                f.write(file_bytes)
                            
                            traces.append({
                                "action": f"Arquivo salvo: {file_name}",
                                "time": time.strftime("%H:%M:%S"),
                                "details": f"Tipo: {file_type}"
                            })
                            
                            
                            if file_type.startswith("image/") or file_name.lower().endswith((".png", ".jpg", ".jpeg", ".gif")):
                                try:
                                    img = Image.open(io.BytesIO(file_bytes))
                                    images.append(img)
                                    traces.append({
                                        "action": f"Imagem processada: {file_name}",
                                        "time": time.strftime("%H:%M:%S"),
                                        "details": f"Dimensões: {img.width}x{img.height}"
                                    })
                                    model_thoughts.append({
                                        "thought": "Gerando visualização para ilustrar os dados encontrados",
                                        "time": time.strftime("%H:%M:%S")
                                    })
                                    
                                    
                                    single_file_span.update(
                                        output={
                                            "width": img.width,
                                            "height": img.height,
                                            "is_image": True
                                        }
                                    )
                                    
                                except Exception as e:
                                    error_msg = str(e)
                                    traces.append({
                                        "action": f"Erro ao processar imagem",
                                        "time": time.strftime("%H:%M:%S"),
                                        "details": error_msg
                                    })
                                    
                                    
                                    single_file_span.update(
                                        level="ERROR",
                                        status_message=f"Image processing error: {error_msg}"
                                    )
                            
                            
                            single_file_span.end()
                            processed_files += 1
                            
                            yield full_response, images, traces, model_thoughts
                        
                        
                        file_span.end(output={"processed_files": processed_files})
                    
                    
                    if "returnControl" in event:
                        roc_span = stream_span.span(name="return-control")
                        
                        data = event["returnControl"]
                        action_group = data['invocationInputs'][0]['functionInvocationInput']['actionGroup']
                        function = data['invocationInputs'][0]['functionInvocationInput']['function']
                        try:
                            value = data['invocationInputs'][0]['functionInvocationInput']['parameters'][0]['value']
                        except:
                            value = None
                        
                        
                        roc_span.update(input={
                            "action_group": action_group,
                            "function": function,
                            "value": value
                        })
                        
                        traces.append({
                            "action": "Evento de controle de retorno detectado",
                            "time": time.strftime("%H:%M:%S")
                        })
                        if value:
                            model_thoughts.append({
                                "thought": f"Executando ferramenta {action_group.split('ActionGroup')[0]} com a função {function} com parâmetro \"{value}\"",
                                "time": time.strftime("%H:%M:%S")
                            })
                        else:
                            model_thoughts.append({
                                "thought": f"Executando ferramenta {action_group.split('ActionGroup')[0]} com a função {function}",
                                "time": time.strftime("%H:%M:%S")
                            })
                        yield full_response, images, traces, model_thoughts
                        
                        
                        process_start_time = time.time()
                        try:
                            inlineSessionState = await process_roc_event(
                                ActionGroups(action_groups=action_groups).tool_map,
                                event,
                                inlineSessionState
                            )
                            process_duration = time.time() - process_start_time
                            
                            
                            roc_span.end(output={
                                "success": True,
                                "duration_ms": round(process_duration * 1000)
                            })
                            
                        except Exception as e:
                            error_msg = str(e)
                            
                            roc_span.end(
                                level="ERROR",
                                status_message=f"ROC processing error: {error_msg}",
                                output={"success": False}
                            )
                        
                        traces.append({
                            "action": "Processamento ROC concluído",
                            "time": time.strftime("%H:%M:%S")
                        })
                        model_thoughts.append({
                            "thought": "Analisando resultados da busca",
                            "time": time.strftime("%H:%M:%S")
                        })
                        yield full_response, images, traces, model_thoughts
                    
                    
                    if "trace" in event and "trace" in event["trace"] and enable_trace:
                        trace_event = stream_span.event(name="process-trace")
                        
                        input_tokens, output_tokens, llm_calls = Trace.parse_trace(
                            trace=event["trace"]["trace"],
                            truncateResponse=truncate_response,
                            agentName="teste",
                        )
                        
                        
                        input_tokens = int(input_tokens)
                        output_tokens = int(output_tokens)
                        llm_calls = int(llm_calls)
                        
                        
                        total_input_tokens += input_tokens
                        total_output_tokens += output_tokens
                        total_llm_calls += llm_calls
                        
                        
                        if input_tokens > 0 or output_tokens > 0:
                            
                            cost_details = calculate_anthropic_costs(
                                model_name=model_name,
                                input_tokens=input_tokens,
                                output_tokens=output_tokens,
                                cache_read_tokens=0  
                            )
                            
                            
                            usage_details = {
                                "input": input_tokens,
                                "output": output_tokens,
                                "total": input_tokens + output_tokens,
                            }
                            
                            
                            model_gen = trace.generation(
                                name=f"model-call-{total_llm_calls}",
                                model=model_name,
                                usage_details=usage_details,
                                cost_details=cost_details,
                                metadata={
                                    "llm_calls": llm_calls,
                                    "iteration": total_llm_calls
                                }
                            )
                        
                        
                        trace_event.output = {
                            "input_tokens": input_tokens,
                            "output_tokens": output_tokens,
                            "llm_calls": llm_calls,
                            "total_tokens": total_input_tokens + total_output_tokens
                        }
                        
                        if input_tokens != 0 or output_tokens != 0 or llm_calls != 0:
                            traces.append({
                                "action": "Métricas atualizadas",
                                "time": time.strftime("%H:%M:%S"),
                                "details": f"Tokens entrada: {total_input_tokens}, Tokens saída: {total_output_tokens}, Chamadas LLM: {total_llm_calls}"
                            })
                        
                        
                        if "trace" in event["trace"] and isinstance(event["trace"]["trace"], dict):
                            trace_data = event["trace"]["trace"]
                            if "orchestrationTrace" in trace_data:
                                orch_trace = trace_data["orchestrationTrace"]
                                
                                
                                if "modelInvocationInput" in orch_trace and "prompt" in orch_trace["modelInvocationInput"]:
                                    prompt = orch_trace["modelInvocationInput"]["prompt"]
                                    if isinstance(prompt, str) and len(prompt) > 0:
                                        model_thoughts.append({
                                            "thought": "Processando informações e formulando resposta",
                                            "time": time.strftime("%H:%M:%S")
                                        })
                                
                                
                                if "observation" in orch_trace:
                                    obs = orch_trace["observation"]
                                    obs_type = obs.get("type", "")
                                    
                                    if obs_type == "KNOWLEDGE_BASE":
                                        model_thoughts.append({
                                            "thought": "Consultando base de conhecimento para informações relevantes",
                                            "time": time.strftime("%H:%M:%S")
                                        })
                                    elif obs_type == "ACTION_GROUP":
                                        model_thoughts.append({
                                            "thought": "Executando ação para obter dados adicionais",
                                            "time": time.strftime("%H:%M:%S")
                                        })
                        
                        yield full_response, images, traces, model_thoughts
                    
                    
                    if "chunk" in event:
                        chunk_span = stream_span.span(name="process-chunk")
                        
                        if add_citation:
                            if "attribution" in event["chunk"]:
                                agent_answer, cite = Trace.add_citation(
                                    citations=event["chunk"]["attribution"]["citations"],
                                    cite=1 if not cite else cite,
                                )
                                
                                
                                chunk_span.update(
                                    metadata={"has_citations": True, "citation_count": cite}
                                )
                                
                                traces.append({
                                    "action": "Citações adicionadas",
                                    "time": time.strftime("%H:%M:%S")
                                })
                                model_thoughts.append({
                                    "thought": "Adicionando citações para as fontes consultadas",
                                    "time": time.strftime("%H:%M:%S")
                                })
                            else:
                                data = event["chunk"]["bytes"]
                                chunk_text = data.decode("utf8")
                                agent_answer += chunk_text
                                full_response += chunk_text
                                
                                
                                chunk_span.update(
                                    metadata={"chunk_size": len(chunk_text)},
                                    output=chunk_text[:100] + ("..." if len(chunk_text) > 100 else "")
                                )
                                
                                traces.append({
                                    "action": "Chunk de resposta recebido",
                                    "time": time.strftime("%H:%M:%S"),
                                    "details": f"Tamanho: {len(chunk_text)} caracteres"
                                })
                                
                                
                                new_thoughts = extract_model_thoughts(chunk_text)
                                for thought in new_thoughts:
                                    model_thoughts.append({
                                        "thought": thought,
                                        "time": time.strftime("%H:%M:%S")
                                    })
                                
                                if not new_thoughts and len(chunk_text) > 50:
                                    model_thoughts.append({
                                        "thought": "Formulando resposta com base nas informações encontradas",
                                        "time": time.strftime("%H:%M:%S")
                                    })
                        elif not add_citation:
                            data = event["chunk"]["bytes"]
                            chunk_text = data.decode("utf8")
                            agent_answer += chunk_text
                            full_response += chunk_text
                            
                            
                            chunk_span.update(
                                metadata={"chunk_size": len(chunk_text)},
                                output=chunk_text[:100] + ("..." if len(chunk_text) > 100 else "")
                            )
                            
                            traces.append({
                                "action": "Chunk de resposta recebido",
                                "time": time.strftime("%H:%M:%S"),
                                "details": f"Tamanho: {len(chunk_text)} caracteres"
                            })
                            
                            
                            new_thoughts = extract_model_thoughts(chunk_text)
                            for thought in new_thoughts:
                                model_thoughts.append({
                                    "thought": thought,
                                    "time": time.strftime("%H:%M:%S")
                                })
                            
                            if not new_thoughts and len(chunk_text) > 50 and len(model_thoughts) < 10:
                                
                                if "?" in message:
                                    model_thoughts.append({
                                        "thought": "Analisando dados para responder sua pergunta com precisão",
                                        "time": time.strftime("%H:%M:%S")
                                    })
                                else:
                                    model_thoughts.append({
                                        "thought": "Organizando informações encontradas para apresentar de forma clara",
                                        "time": time.strftime("%H:%M:%S")
                                    })
                        
                        
                        chunk_span.end()
                        
                        yield full_response, images, traces, model_thoughts
                
                
                stream_span.end(output={"success": True})
            
            except Exception as e:
                error_msg = f"Erro inesperado: {str(e)}"
                traces.append({
                    "action": "Erro",
                    "time": time.strftime("%H:%M:%S"),
                    "details": error_msg
                })
                model_thoughts.append({
                    "thought": f"Encontrei um problema: {error_msg}",
                    "time": time.strftime("%H:%M:%S")
                })
                
                
                stream_span.end(
                    level="ERROR",
                    status_message=error_msg,
                    output={"error": str(e)}
                )
                
                full_response += f"\n\nOcorreu um erro durante o processamento: {error_msg}"
                yield full_response, images, traces, model_thoughts
                raise Exception("Unexpected exception: ", e)
        
        
        orchestration_span.end(output={
            "success": True,
            "response_length": len(full_response)
        })
        
        
        cleanup_span = trace.span(name="cleanup-mcp-clients")
        if internet_search_mcp_client:
            try:
                await internet_search_mcp_client.cleanup()
            except Exception as e:
                cleanup_span.event(
                    name="cleanup-error",
                    level="WARNING", 
                    status_message=f"Internet search client cleanup error: {str(e)}"
                )
                
        if location_mcp_client:
            try:
                await location_mcp_client.cleanup()
            except Exception as e:
                cleanup_span.event(
                    name="cleanup-error",
                    level="WARNING", 
                    status_message=f"Location client cleanup error: {str(e)}"
                )
                
        if dynamo_mcp_client:
            try:
                await dynamo_mcp_client.cleanup()
            except Exception as e:
                cleanup_span.event(
                    name="cleanup-error",
                    level="WARNING", 
                    status_message=f"Dynamo client cleanup error: {str(e)}"
                )
        
        
        cleanup_span.end()
        
        traces.append({
            "action": "Processamento concluído",
            "time": time.strftime("%H:%M:%S"),
            "details": f"Tokens totais: {total_input_tokens + total_output_tokens}"
        })
        model_thoughts.append({
            "thought": "Resposta completa formulada com base nas informações encontradas",
            "time": time.strftime("%H:%M:%S")
        })
        
        
        total_costs = calculate_anthropic_costs(
            model_name=model_name,
            input_tokens=total_input_tokens,
            output_tokens=total_output_tokens,
            cache_read_tokens=total_cache_read_tokens
        )
        
        
        final_usage_details = {
            "input": total_input_tokens,
            "output": total_output_tokens,
            "cache_read_input_tokens": total_cache_read_tokens,
            "total": total_input_tokens + total_output_tokens + total_cache_read_tokens,
        }
        
        
        trace.generation(
            name="session-summary",
            model=model_name,
            input=message,
            output=full_response[:1000] + ("..." if len(full_response) > 1000 else ""),
            usage_details=final_usage_details,
            cost_details=total_costs,
            metadata={
                "total_llm_calls": total_llm_calls,
                "total_images": len(images)
            }
        )
        
        
        trace.update(
            output=full_response,
            metadata={
                "model": model_name,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "total_llm_calls": total_llm_calls,
                "total_images": len(images),
                "total_cost": total_costs["total"]
            }
        )
        
        yield full_response, images, traces, model_thoughts
        
    except Exception as e:
        error_msg = f"Erro: {str(e)}"
        if not full_response:
            full_response = f"Desculpe, ocorreu um erro durante o processamento: {error_msg}"
        
        
        trace.update(
            level="ERROR",
            status_message=error_msg,
            output=full_response
        )
        
        yield full_response, images, traces, model_thoughts
    finally:
        
        if internet_search_mcp_client:
            try:
                await internet_search_mcp_client.cleanup()
            except Exception as e:
                pass   
        if location_mcp_client:
            try:
                await location_mcp_client.cleanup()
            except Exception as e:
                pass
        if dynamo_mcp_client:
            try:
                await dynamo_mcp_client.cleanup()
            except Exception as e:
                pass

def format_traces(traces):
    """Formata os traces para exibição"""
    if not traces:
        return ""
    
    html = "<div style='max-height: 400px; overflow-y: auto;'>"
    html += "<ul style='list-style-type: none; padding-left: 0;'>"
    
    for trace in traces:
        action = trace.get("action", "")
        details = trace.get("details", "")
        timestamp = trace.get("time", time.strftime("%H:%M:%S"))
        
        html += f"<li style='margin-bottom: 8px; padding: 6px; border-left: 3px solid #3498db; background-color: #f8f9fa;'>"
        html += f"<span style='font-weight: bold; color: #3498db;'>{timestamp}</span><br>"
        html += f"<span style='font-weight: 500;'>{action}</span>"
        
        if details:
            html += f"<br><span style='color: #555; font-size: 0.9em;'>{details}</span>"
        
        html += "</li>"
    
    html += "</ul></div>"
    return html

def format_thoughts(thoughts):
    """Formata os pensamentos do modelo para exibição"""
    if not thoughts:
        return ""
    
    html = "<div style='max-height: 400px; overflow-y: auto;'>"
    html += "<ul style='list-style-type: none; padding-left: 0;'>"
    
    for thought in thoughts:
        thought_text = thought.get("thought", "")
        timestamp = thought.get("time", time.strftime("%H:%M:%S"))
        
        html += f"<li style='margin-bottom: 8px; padding: 6px; border-left: 3px solid #28a745; background-color: #f8f9fa;'>"
        html += f"<span style='font-weight: bold; color: #28a745;'>{timestamp}</span><br>"
        html += f"<span style='font-weight: 500;'>{thought_text}</span>"
        html += "</li>"
    
    html += "</ul></div>"
    return html

def chat_response(message, model_name, history):
    """Função principal para lidar com o chat"""
    print(f"\n=== STARTING chat_response for: {message} with model: {model_name} ===")
    
    
    chat_trace = langfuse.trace(
        name="chat-interaction",
        input=message,
        metadata={"history_length": len(history) if history else 0, "model": model_name}
    )
    
    
    loop = asyncio.new_event_loop()
    
    
    async_gen = agent_process(message, model_name=model_name)
    
    
    try:
        while True:
            
            try:
                result = loop.run_until_complete(process_single_iteration(async_gen))
                if result is None:  
                    break
                
                full_response, images, traces, model_thoughts = result
                trace_html = format_traces(traces)
                thoughts_html = format_thoughts(model_thoughts)
                
                
                chat_trace.event(
                    name="response-progress",
                    output={
                        "response_length": len(full_response) if full_response else 0,
                        "images_count": len(images) if images else 0
                    }
                )
                
                yield full_response, images, trace_html, thoughts_html
            except StopAsyncIteration:
                break
    finally:
        
        if 'full_response' in locals() and full_response:
            chat_trace.update(
                output=full_response[:1000] + ("..." if len(full_response) > 1000 else ""),
                metadata={
                    "model": model_name,
                    "images_count": len(images) if 'images' in locals() else 0,
                    "final_response_length": len(full_response) if 'full_response' in locals() else 0
                }
            )
        
        
        loop.close()

def respond(message, model_name, chat_history, trace_state, update_trigger):
    """Handler for Gradio interface"""
    bot_message = ""
    images = []
    traces = []
    thoughts = []
    
    chat_history.append([message, None])
    
    response_generator = chat_response(message, model_name, chat_history)
    
    for response_text, current_images, trace_html, thoughts_html in response_generator:
        bot_message = response_text
        images = current_images
        
        chat_history[-1][1] = bot_message
        
        new_trigger = update_trigger + 1
        yield chat_history, images, trace_html, thoughts_html, new_trigger
    
    
    trace_id = str(uuid.uuid4())  
    langfuse.event(
        trace_id=trace_id,
        name="response-delivered",
        metadata={"response_length": len(bot_message) if bot_message else 0}
    )
    
    return chat_history, images, trace_html, thoughts_html, update_trigger + 1



with gr.Blocks(theme=gr.themes.Soft()) as demo:
    update_trigger = gr.State(0)
    
    with gr.Row():
        gr.Markdown("""
        
        **Navegue por dados, produtos e rotas com inteligência**
        """)
    
    with gr.Row():
        
        with gr.Column(scale=1):
            with gr.Accordion("🧠 Pensamentos do Modelo", open=True):
                thoughts_display = gr.HTML(
                    label="Raciocínio", 
                    elem_id="thoughts-display",
                    value="<div style='height: 650px; overflow-y: auto;'></div>"
                )
        
        
        with gr.Column(scale=2):
            
            
            chatbot = gr.Chatbot(
                height=450,
                bubble_full_width=False,
                avatar_images=(
                    "https://cdn-icons-png.flaticon.com/512/1077/1077114.png", 
                    "https://cdn-icons-png.flaticon.com/512/4712/4712035.png"
                ),
                elem_id="chatbot"
            )
            msg = gr.Textbox(
                label="Sua pergunta", 
                placeholder="Digite sua consulta...",
                scale=4
            )
            clear_btn = gr.Button("Limpar conversa", scale=1)

            model_selection = gr.Dropdown(
                choices=[
                    "us.anthropic.claude-3-5-sonnet-20241022-v2:0",
                    "us.anthropic.claude-3-5-haiku-20241022-v1:0"
                ],
                value="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
                label="Selecione o modelo",
                info="Escolha entre Claude 3.5 Sonnet (mais poderoso) ou Claude 3.5 Haiku (mais rápido)"
            )
        
        
        with gr.Column(scale=1):
            with gr.Accordion("📊 Imagens Geradas", open=True):
                image_display = gr.Gallery(
                    label="Visualização", 
                    columns=1, 
                    height=250, 
                    object_fit="contain"
                )
            
            with gr.Accordion("🔍 Trace Técnico", open=True):
                trace_display = gr.HTML(
                    label="Ações do Sistema",
                    elem_id="trace-display",
                    value="<div style='height: 380px; overflow-y: auto;'></div>"
                )
    
    trace_state = gr.State([])
    
    
    msg.submit(
        fn=respond,
        inputs=[msg, model_selection, chatbot, trace_state, update_trigger],
        outputs=[chatbot, image_display, trace_display, thoughts_display, update_trigger],
        queue=True
    ).then(
        lambda: "", 
        None, 
        msg
    )
    
    clear_btn.click(
        lambda trigger: ([], [], "", "", trigger + 1),
        inputs=[update_trigger],
        outputs=[chatbot, image_display, trace_display, thoughts_display, update_trigger]
    )
    
    
    css = """
    <style>
    /* Estilo para os componentes HTML */
    
        width: 100%;
        max-width: 100%;
        padding: 0;
        margin: 0;
    }
    
    /* Estilo para o conteúdo dentro dos componentes HTML */
    
        width: 100%;
        padding: 8px;
        box-sizing: border-box;
        font-size: 0.9em;
    }
    
    /* Estilo para o chatbot */
    
        border-left: 1px solid 
        border-right: 1px solid 
    }
    
    /* Ajustes para as colunas */
    .gr-column {
        padding: 0 8px;
    }
    
    /* Estilo para os acordeões */
    .gr-accordion {
        margin-bottom: 10px;
    }
    </style>
    """
    
    gr.HTML(value=css, visible=False)
    
    
    def format_traces(traces):
        """Formata os traces para exibição"""
        if not traces:
            return "<div style='height: 380px; overflow-y: auto;'></div>"
        
        html = "<div style='height: 380px; overflow-y: auto;'>"
        html += "<ul style='list-style-type: none; padding-left: 0;'>"
        
        for trace in traces:
            action = trace.get("action", "")
            details = trace.get("details", "")
            timestamp = trace.get("time", time.strftime("%H:%M:%S"))
            
            html += f"<li style='margin-bottom: 8px; padding: 6px; border-left: 3px solid #3498db; background-color: #f8f9fa;'>"
            html += f"<span style='font-weight: bold; color: #3498db;'>{timestamp}</span><br>"
            html += f"<span style='font-weight: 500;'>{action}</span>"
            
            if details:
                html += f"<br><span style='color: #555; font-size: 0.9em;'>{details}</span>"
            
            html += "</li>"
        
        html += "</ul></div>"
        return html

    def format_thoughts(thoughts):
        """Formata os pensamentos do modelo para exibição"""
        if not thoughts:
            return "<div style='height: 650px; overflow-y: auto;'></div>"
        
        html = "<div style='height: 650px; overflow-y: auto;'>"
        html += "<ul style='list-style-type: none; padding-left: 0;'>"
        
        for thought in thoughts:
            thought_text = thought.get("thought", "")
            timestamp = thought.get("time", time.strftime("%H:%M:%S"))
            
            html += f"<li style='margin-bottom: 8px; padding: 6px; border-left: 3px solid #28a745; background-color: #f8f9fa;'>"
            html += f"<span style='font-weight: bold; color: #28a745;'>{timestamp}</span><br>"
            html += f"<span style='font-weight: 500;'>{thought_text}</span>"
            html += "</li>"
        
        html += "</ul></div>"
        return html
    
    demo.load(None)

if __name__ == "__main__":
    try:
        demo.launch(
            server_name="0.0.0.0",
            server_port=7860,
            share=False,
            show_error=True
        )
    finally:
        langfuse.flush()
        print("Langfuse events flushed")
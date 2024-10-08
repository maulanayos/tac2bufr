FROM condaforge/mambaforge:latest

# Set up the environment
COPY tac2bufr/environment.yml /tmp/env.yaml
COPY shared-tools /shared-tools

# Create the conda environment
RUN conda env create -f /tmp/env.yaml && \
    conda clean --all --yes

# Set the PATH to include the new environment
RUN export ENV_NAME=$(head -n 1 /tmp/env.yaml | cut -d: -f2 | tr -d ' ') && \
    echo "export PATH=/opt/conda/envs/$ENV_NAME/bin:$PATH" >> ~/.bashrc
# Copy the application code
COPY tac2bufr .

# Activate the environment, install pip dependencies, and run the application
CMD ["bash", "-c", "export ENV_NAME=$(head -n 1 /tmp/env.yaml | cut -d: -f2 | tr -d ' ') && \
    source activate $ENV_NAME && \
    pip install -r requirements.txt && \
    python main.py"]

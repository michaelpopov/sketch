#include "data_command_processor.h"
#include "engine.h"
#include "input_data.h"
#include "string_utils.h"
#include "log.h"
#include "ivf_builder.h"
#include <format>
#include <iostream>
#include <sstream>

namespace sketch {

static CommandNames supported_commands = { "USE", "GENERATE", "LOAD", "DUMP", "FIND", "KNN",
                                           "SAMPLE", "KMEANS++", "MAKE_CENTROIDS", "MAKE_IVF",
                                           "ANN", "GC", "SHOW_IVF" };

DataCommandProcessor::DataCommandProcessor(Engine& engine)
  : engine_(engine) {

}

DataCommandProcessor::~DataCommandProcessor() {

}

const CommandNames& DataCommandProcessor::get_supported_commands() const {
    return supported_commands;
}

Ret DataCommandProcessor::process_command(Commands& commands, bool is_help) {
    if (commands.empty()) {
        return "No command to process";
    }

    const auto& cmd_type = commands[0];

    if (cmd_type == "USE") {
        return process_use_cmd(commands, is_help);
    } else if (cmd_type == "GENERATE") {
        return process_generate_cmd(commands, is_help);
    } else {
        if (!current_dataset_) {
            return "No dataset selected. Use the USE command to select a dataset.";
        }

        if (cmd_type == "LOAD") {
            return process_load_cmd(commands, is_help);
        } else if (cmd_type == "DUMP") {
            return process_dump_cmd(commands, is_help);
        } else if (cmd_type == "FIND") {
            return process_find_cmd(commands, is_help);
        } else if (cmd_type == "KNN") {
            return process_knn_cmd(commands, is_help);
        } else if (cmd_type == "SAMPLE") {
            return process_sample_cmd(commands, is_help);
        } else if (cmd_type == "KMEANS++") {
            return process_kmeanspp_cmd(commands, is_help);
        } else if (cmd_type == "MAKE_CENTROIDS") {
            return process_make_centroids_cmd(commands, is_help);
        } else if (cmd_type == "MAKE_IVF") {
            return process_make_ivf_cmd(commands, is_help);
        } else if (cmd_type == "SHOW_IVF") {
            return process_show_ivf_cmd(commands, is_help);
        } else if (cmd_type == "ANN") {
            return process_ann_cmd(commands, is_help);
        } else if (cmd_type == "GC") {
            return process_gc_cmd(commands, is_help);
        }
    }

    return std::string("Unknown DATA command type: ") + std::string(cmd_type);
}

Ret DataCommandProcessor::process_use_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "USE command help: USE <catalog_name>.<dataset_name>;");
    }

    if (commands.size() < 2) {
        return "USE command requires additional parameters";
    }

    std::vector<std::string_view> parts;
    split_string(commands[1], '.', parts);
    if (parts.size() != 2) {
        return "Dataset name must be in the format <catalog_name>.<dataset_name>";
    }

    current_dataset_ = engine_.find_dataset(parts[0], parts[1]);
    if (!current_dataset_) {
        return std::format("Dataset {}.{} not found", parts[0], parts[1]);
    }

    return Ret(0, std::format("Using dataset {}.{}", parts[0], parts[1]));
}

Ret DataCommandProcessor::process_generate_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "GENERATE command help: GENERATE <path> <count> <dim> <start>;");
    }

    if (commands.size() < 3) {
        return "GENERATE command requires additional parameters";
    }

    const auto& path = commands[1];

    size_t count = 0;
    try {
        count = u64_from_string_view(commands[2]);
    } catch (const std::exception& e) {
        return std::format("Failed to parse COUNT parameter: {}", commands[2]);
    }

    size_t dim = 128;
    if (commands.size() > 3) {
        try {
            dim = u64_from_string_view(commands[3]);
        } catch (const std::exception& e) {
            return "Failed to parse DIM parameter";
        }
    }

    size_t start = 0;
    if (commands.size() > 4) {
        try {
            start = u64_from_string_view(commands[4]);
        } catch (const std::exception& e) {
            return "Failed to parse START parameter";
        }
    }

    int ret = InputDataGenerator::generate(path, dim, count, start);
    if (ret != 0) {
        return "Failed to generate test data in dataset";
    }

    return Ret(0, std::format("Generated {} test data items in {}", count, path));
}

Ret DataCommandProcessor::process_load_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "LOAD command help: LOAD <input_path>;");
    }

    if (commands.size() < 2) {
        return "LOAD command requires additional parameters";
    }

    const auto& input_path = commands[1];

    LoadReport report;
    int ret = current_dataset_->load(input_path, report, engine_.thread_pool());
    if (ret != 0) {
        return ret;
    }

    LOG_DEBUG << "input_count=" << report.input_count.load();
    LOG_DEBUG << "staged_count=" << report.staged_count.load();
    LOG_DEBUG << "staged_read_count=" << report.staged_read_count.load();
    LOG_DEBUG << "added_count=" << report.added_count.load();
    LOG_DEBUG << "removed_count=" << report.removed_count.load();
    LOG_DEBUG << "updated_count=" << report.updated_count.load();
    LOG_DEBUG << "nodes_count=" << report.nodes_count.load();
    LOG_DEBUG << "conversion_errors_count=" << report.conversion_errors_count.load();
    LOG_DEBUG << "processed_count=" << report.processed_count.load();

    std::stringstream stream;
    stream << "Loaded " << report.processed_count.load() << " / " << report.input_count << " items into dataset\n";
    stream << " - added: " << report.added_count.load() << "\n";
    stream << " - removed: " << report.removed_count.load() << "\n";
    stream << " - updated: " << report.updated_count.load() << "\n";

    return Ret(0, stream.str(), true);
}

Ret DataCommandProcessor::process_dump_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "DUMP command help: DUMP [<path>]");
    }

    std::string_view path = "";
    if (commands.size() > 1) {
        path = commands[1];
    }

    return current_dataset_->dump(path, engine_.thread_pool());
}

Ret DataCommandProcessor::process_find_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "FIND command help: FIND [TAG <tag>] | [DATA #<id>] <path>");
    }

    if (commands.size() < 3) {
        return "FIND command requires additional parameters";
    }

    const std::string_view& command_type = commands[1];
    const std::string_view& command_param = commands[2];

    if (command_type == "TAG") {
        PARAMS(tag, command_param);
        return current_dataset_->find_tag(tag, engine_.thread_pool());
    }

    if (command_type == "DATA") {
        if (commands.size() < 4) {
            return "FIND DATA command requires additional parameters";
        }

        const std::string_view& path = commands[3];

        auto input_data = std::make_unique<InputData>();
        if (input_data->init(path) != 0) {
            return "Failed to initialize test data";
        }

        if (command_param.size() < 2 || command_param[0] != '#') {
            return "Invalid test data reference.";
        }

        PARAMS(index, std::string_view(command_param.data()+1, command_param.size()-1));

        uint64_t _;
        std::vector<uint8_t> data;
        int ret = input_data->get(index, current_dataset_->metadata(), _, data);
        if (ret != 0) {
            return "Failed to parse get test data.";
        }

        return current_dataset_->find_data(data, engine_.thread_pool());
    }

    return "Invalid FIND command";
}

Ret DataCommandProcessor::process_knn_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "KNN command help: KNN L1|L2|COS <count> #<id> path");
    }

    if (commands.size() < 5) {
        return "KNN command requires additional parameters";
    }

    const std::string_view& type_param = commands[1];

    KnnType type = KnnType::Undefined;
    if (type_param == "L1") {
        type = KnnType::L1;
    } else if (type_param == "L2") {
        type = KnnType::L2;
    } else if (type_param == "COS") {
        type = KnnType::COS;
    } else {
        return "Invalid KNN type";
    }

    PARAM(2, count);

    const std::string_view& id_param = commands[3];
    if (id_param.size() < 2 || id_param[0] != '#') {
        return "Invalid test data reference";
    }

    PARAMS(index, std::string_view(id_param.data()+1, id_param.size()-1));

    const std::string_view& path = commands[4];
    auto input_data = std::make_unique<InputData>();
    if (input_data->init(path) != 0) {
        return "Failed to initialize test data";
    }

    uint64_t tag;
    std::vector<uint8_t> data;
    int ret = input_data->get(index, current_dataset_->metadata(), tag, data);
    if (ret != 0) {
        return "Failed to parse get test data.";
    }

    return current_dataset_->knn(type, count, data, tag, engine_.thread_pool());
}

Ret DataCommandProcessor::process_sample_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "SAMPLE command help: SAMPLE <count>");
    }

    if (commands.size() < 2) {
        return "SAMPLE command requires additional parameters";
    }

    PARAM(1, records_count);

    const uint32_t centroids_count = 0;
    const auto& md = current_dataset_->metadata();
    IvfBuilder builder(md.type, md.dim, centroids_count, records_count);
    auto ret = builder.init();
    if (ret != 0) {
        return ret;
    }

    ret = current_dataset_->sample_records(builder, engine_.thread_pool());
    if (ret != 0) {
        return ret;
    }

    std::stringstream stream;
    for (size_t i = 0; i < records_count && i < 16; i++) {
        const auto& record = builder.get_record(i);

        for (size_t d = 0; d < md.dim && d < 4; d++) {
            switch (md.type) {
                case DatasetType::f32: {
                    const float* f = reinterpret_cast<const float*>(record);
                    stream << f[d] << ", ";
                    break;
                }
                case DatasetType::f16: {
                    const float16_t* f = reinterpret_cast<const float16_t*>(record);
                    stream << f[d] << ", ";
                    break;
                }
            }
        }
        stream << "\n";
    }

    return Ret(0, stream.str(), true);
}

Ret DataCommandProcessor::process_kmeanspp_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "KMEANS++ command help: KMEANS++ <centroids_count> <sample_size>");
    }

    if (commands.size() < 3) {
        return "KMEANS++ command requires additional parameters";
    }

    PARAM(1, centroids_count);
    PARAM(2, sample_size);

    const auto& md = current_dataset_->metadata();
    IvfBuilder builder(md.type, md.dim, centroids_count, sample_size);
    int ret = builder.init();
    if (ret != 0) {
        return ret;
    }

    ret = current_dataset_->init_centroids_kmeans_plus_plus(builder, engine_.thread_pool());
    if (ret != 0) {
        return ret;
    }

    std::stringstream stream;
    stream << std::endl;
    for (size_t i = 0; i < centroids_count; i++) {
        const uint8_t* c = builder.get_centroid(i);
        switch (md.type) {
            case DatasetType::f16: {
                const float16_t* data_ptr = reinterpret_cast<const float16_t*>(c);
                for (size_t j = 0; j < 3; j++) {
                    stream << data_ptr[j] << " ";
                }
                break;
            }
            case DatasetType::f32: {
                const float* data_ptr = reinterpret_cast<const float*>(c);
                for (size_t j = 0; j < 3; j++) {
                    stream << data_ptr[j] << " ";
                }
                break;
            }
        }
        stream << std::endl;
    }

    return Ret(0, stream.str(), true);
}

Ret DataCommandProcessor::process_make_centroids_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "MAKE_CENTROIDS command help: MAKE_CENTROIDS <centroids_count> <sample_size> <recalc_count>");
    }

    if (commands.size() < 4) {
        return "MAKE_CENTROIDS command requires additional parameters";
    }

    std::stringstream stream;
    stream << std::endl;

    PARAM(1, centroids_count);
    PARAM(2, sample_size);
    PARAM(3, recalc_count);

    DatasetHolder holder(*current_dataset_);
    if (holder.is_shutting_down()) {
        return "Dataset is shutting down";
    }

    const auto& md = current_dataset_->metadata();
    IvfBuilder builder(md.type, md.dim, centroids_count, sample_size);
    int ret = builder.init();
    if (ret != 0) {
        return ret;
    }

    ret = current_dataset_->init_centroids_kmeans_plus_plus(builder, engine_.thread_pool());
    if (ret != 0) {
        return ret;
    }

    for (uint64_t i = 0; i < recalc_count/2 + 1; i++) {
        ret = builder.recalc_centroids();
        if (ret != 0) {
            return ret;
        }
    }

    for (size_t i = 0; i < centroids_count; i++) {
        const uint8_t* c = builder.get_centroid(i);
        switch (md.type) {
            case DatasetType::f16: {
                const float16_t* data_ptr = reinterpret_cast<const float16_t*>(c);
                for (size_t j = 0; j < 3; j++) {
                    stream << data_ptr[j] << ", ";
                }
                break;
            }
            case DatasetType::f32: {
                const float* data_ptr = reinterpret_cast<const float*>(c);
                for (size_t j = 0; j < 3; j++) {
                    stream << data_ptr[j] << ", ";
                }
                break;
            }
        }
        stream << std::endl;
    }

    return Ret(0, stream.str(), true);
}

Ret DataCommandProcessor::process_make_ivf_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "MAKE_IVF command help: MAKE_IVF <centroids_count> <sample_size> <recalc_count>");
    }

    if (commands.size() < 4) {
        return "MAKE_IVF command requires additional parameters";
    }

    PARAM(1, centroids_count);
    PARAM(2, sample_size);
    PARAM(3, recalc_count);

    DatasetHolder holder(*current_dataset_);
    if (holder.is_shutting_down()) {
        return "Dataset is shutting down";
    }

    const auto& md = current_dataset_->metadata();
    IvfBuilder builder(md.type, md.dim, centroids_count, sample_size);
    int ret = builder.init();
    if (ret != 0) {
        return ret;
    }

    ret = current_dataset_->init_centroids_kmeans_plus_plus(builder, engine_.thread_pool());
    if (ret != 0) {
        return ret;
    }

    for (uint64_t i = 0; i < recalc_count / 2 + 1; i++) {
        ret = builder.recalc_centroids();
        if (ret != 0) {
            return ret;
        }
    }

    return current_dataset_->write_index(builder, engine_.thread_pool());
}

Ret DataCommandProcessor::process_show_ivf_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "SHOW_IVF command help: SHOW_IVF");
    }

    if (commands.size() != 1) {
        return "SHOW_IVF command does not require additional parameters";
    }

    return current_dataset_->show_ivf();
}


Ret DataCommandProcessor::process_ann_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "ANN command help: ANN <count> nprobes #<id> path");
    }

    if (commands.size() < 5) {
        return "ANN command requires additional parameters";
    }

    PARAM(1, count);
    PARAM(2, nprobes);

    const std::string_view& id_param = commands[3];
    if (id_param.size() < 2 || id_param[0] != '#') {
        return "Invalid test data reference";
    }

    PARAMS(index, std::string_view(id_param.data()+1, id_param.size()-1));

    const std::string_view& path = commands[4];
    auto input_data = std::make_unique<InputData>();
    if (input_data->init(path) != 0) {
        return "Failed to initialize test data";
    }

    uint64_t tag;
    std::vector<uint8_t> data;
    int ret = input_data->get(index, current_dataset_->metadata(), tag, data);
    if (ret != 0) {
        return "Failed to parse get test data.";
    }

    return current_dataset_->ann(count, nprobes, data, tag, engine_.thread_pool());
}

Ret DataCommandProcessor::process_gc_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "GC command help: GC");
    }

    if (commands.size() != 1) {
        return "GC command does not require additional parameters";
    }

    return current_dataset_->gc();
}

} // namespace sketch
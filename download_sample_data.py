
from pathlib import Path
from torchvision.datasets import CIFAR10

# Save sample images inside the ingestion input folder.
project_dir = Path(__file__).resolve().parent
output_dir = project_dir / "data" / "incoming"
download_dir = project_dir / "data" / "cifar10_download"

output_dir.mkdir(parents=True, exist_ok=True)
download_dir.mkdir(parents=True, exist_ok=True)

print("Downloading CIFAR-10 training data...")
dataset = CIFAR10(
    root=str(download_dir),
    train=True,
    download=True
)

# Export the first 50 images as individual PNG files.
for index in range(50):
    image, label = dataset[index]
    class_name = dataset.classes[label]
    filename = f"{index:04d}_{class_name}.png"
    image.save(output_dir / filename)

print(f"Exported 50 images to: {output_dir}")
print("Classes:", ", ".join(dataset.classes))
